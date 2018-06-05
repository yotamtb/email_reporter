import datetime
import email
import argparse
import imaplib
import os
import sys

import boto3

import config
import db_handler

# How many days back we need to fetch emails
DAYS_BACK = 2
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
        is_24_hours_verified = False if (DAYS_BACK > 0 and data[0].split()) else True
        mail_ids = data[0].split() or xrange(num_of_mails - FETCH_NUM_FALLBACK,
                                             num_of_mails)
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
                yield {'id': num,
                       'message': actual_message,
                       'metadata': {header: mail_obj[header] for header in
                                         ['subject', 'to', 'from', 'date']}}
    except Exception as err:
        print str(err)
        raise


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
    """
    opens a connection to S3 based on a given access key id and secret
    :return: S3 open session object
    """
    session = boto3.Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
    s3 = session.resource('s3')
    print "Connected to S3 successfully"
    return s3


def upload_mails_to_s3(s3_session, email_id, email_message):
    """
    uploads fetched email to s3
    :param email_id: The id of the email as given from the server
    :param email_message: The email message
    """
    object = s3_session.Object(config.BUCKET_NAME, config.EMAIL_DOWNLOAD_FILE_NAME.format(email_id))
    object.put(Body=email_message)
    s3_link = os.path.join('https://s3.amazonaws.com',object.bucket_name,object.key)
    print "email #{n} is at {p}".format(n=email_id,
                                        p=s3_link)
    return s3_link


def run(upload=False, report=False, local_path=None):
    try:
        email_acc = email_login(config.EMAIL_ACCOUNT, config.EMAIL_PASSWORD)
        report_data = []
        s3_link = None
        if upload:
            s3_session = connect_to_s3()
        for fetched_email_data in fetch_emails(email_acc):
            if upload:
                s3_link = upload_mails_to_s3(s3_session,
                                             fetched_email_data['id'],
                                             fetched_email_data['message'])
            else:
                save_email_as_file(local_path,
                                   fetched_email_data['id'],
                                   fetched_email_data['message'])
            if report:
                fetched_email_data['metadata']['id'] = int(fetched_email_data['id'])
                fetched_email_data['metadata']['s3_link'] = s3_link
                report_data.append(fetched_email_data['metadata'])
            if len(report_data) == config.REPORT_BULK_SIZE:
                db_handler.upsert_report(report_data)
        if report_data:
            db_handler.upsert_report(report_data)
    except Exception as e:
        print str(e)
        raise
    finally:
        email_acc.close()
        email_acc.logout()


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-d', '--download_path',
            help='Local path where emails will be downloaded to. Mandatory')
        parser.add_argument(
            '--upload',
            action="store_true",
            help='Upload the mails to a pre-defined S3 bucket'
        )
        parser.add_argument(
            '--report',
            action="store_true",
            help='Produce a report with the fetched emails details'
        )
        args = parser.parse_args()
        assert args.upload or args.download_path, \
            'You must provide a local path to download email messages or ' \
            'use --upload to upload files to S3 bucket'
        run(upload=args.upload,report=args.report, local_path=args.download_path)
        print "Finished successfully"
    except Exception as ex:
        print ex.message
        sys.exit(1)
