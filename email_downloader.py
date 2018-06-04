import datetime
import email
import argparse
import imaplib
import os
import sys

import boto3

import config

# How many days back we need to fetch emails
DAYS_BACK = 1
# How many emails to fetch in case we didn't find
FETCH_NUM_FALLBACK = 10


def email_login(user, password):
    """
    login to an email account with given username and password
    :param user: username of the email account
    :param password: password of the email account
    :return: imap session
    """
    try:
        M = imaplib.IMAP4_SSL(config.EMAIL_SERVER)
        M.login(user, password)
        print "login successfully"
        return M
    except Exception as err:
        print str(err)
        raise


def verify_x_hours(mail_date, hours):
    """
    Verify the mail was sent within the previously x hours
    (email query allows us to search mails by date only)
    :param mail_date: The date the mail was sent
    :param hours: How many hours back we need to check
    :return: boolean - Was the mail sent in the last x hours
    """
    parsed_date = email.utils.parsedate_tz(mail_date)
    utc_timestamp = email.utils.mktime_tz(parsed_date)
    utc_datetime = datetime.datetime.utcfromtimestamp(utc_timestamp)
    utc_now = datetime.datetime.utcnow()
    return utc_datetime >= utc_now - datetime.timedelta(hours=hours)


def fetch_emails(email_account_obj):
    """
    Build the search query and fetch email from days_back.
    If no emails found from the last DAYS_BACK days - fetch the
    last FETCH_NUM_FALLBACK mails.
    :param email_account_obj: The email account we logged in
    :return: email id and message
    """
    try:
        _, num_of_mails = email_account_obj.select()
        num_of_mails = int(num_of_mails[0])
        fetch_val = (datetime.datetime.today() -
                     datetime.timedelta(days=DAYS_BACK)).strftime(
            config.EMAIL_QUERY_DATE_FORMAT)
        _, data = email_account_obj.search(None,
                                           '(SINCE "{}")'.format(fetch_val))
        # If we didn't find any mails we will fetch the last y mails.
        # In that case we do not need to perform the x hours check
        is_24_hours_verified, mail_ids = (False,
                                          data[0].split()) or \
                                         (True,
                                          xrange(
                                              num_of_mails - FETCH_NUM_FALLBACK,
                                              num_of_mails))
        for num in mail_ids:
            _, msg_data = email_account_obj.fetch(num, '(RFC822)')
            actual_message = msg_data[0][1]
            mail_obj = email.message_from_string(actual_message)
            mail_date = mail_obj['date']
            # Because the mails are sorted by date we can skip the test after
            # finding the first email that was sent in the last x hours
            if is_24_hours_verified or verify_x_hours(mail_date,
                                                      hours=DAYS_BACK * 24):
                is_24_hours_verified = True
                print "fetched email #{}".format(num)
                yield num, mail_obj.as_string()
    except Exception as err:
        print str(err)
        raise
    finally:
        email_account_obj.close()
        email_account_obj.logout()


def save_email_as_file(email_download_path, email_id, email_msg):
    """
    Save the email message to a file in the given path with
    name format 'email_id_<id>.eml'
    :param email_id: The id of the email as given from the server
    :param email_msg: The email message
    :return: None
    """
    if not os.path.isdir(email_download_path):
        os.makedirs(email_download_path)
    with open(os.path.join(email_download_path,
                           config.EMAIL_DOWNLOAD_FILE_NAME.format(email_id)),
              'wt') as email_file:
        email_file.write(email_msg)
        print "email #{n} is at {p}".format(n=email_id,
                                            p=email_file.name)


def connect_to_s3():
    session = boto3.Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
    s3 = session.resource('s3')
    return s3


def upload_mails_to_s3(email_id, email_message):
    s3 = connect_to_s3()
    object = s3.Object(config.BUCKET_NAME, config.EMAIL_DOWNLOAD_FILE_NAME.format(email_id))
    object.put(Body=email_message)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--download_path',
        help='Local path where emails will be downloaded to. Mandatory')
    parser.add_argument(
        '--upload',
        action="store_true",
        help='Upload the mails to a pre-defined S3 bucket'
    )
    args = parser.parse_args()
    try:
        assert args.upload or args.download_path, \
            'You must provide a local path to download email messages or ' \
            '--upload to upload files to S3 bucket'
        email_acc = email_login(config.EMAIL_ACCOUNT, config.EMAIL_PASSWORD)
        for fetched_email_id, fetched_email_message in fetch_emails(email_acc):
            if args.download_path:
                save_email_as_file(args.download_path, fetched_email_id,
                                   fetched_email_message)
            if args.upload:
                upload_mails_to_s3(fetched_email_id, fetched_email_message)
        print "Finished successfully"
    except Exception as ex:
        print ex.message
        sys.exit(1)
