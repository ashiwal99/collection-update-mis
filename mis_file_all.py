import pandas as pd
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import sys
import numpy as np
import keyring
from smtplib import (
    SMTPAuthenticationError,
    SMTPConnectError,
    SMTPRecipientsRefused,
    SMTPServerDisconnected,
    SMTPException,
)
import socket
import logging
import traceback

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\Temp Files\logs\mis_files.log", encoding='utf-8')
console_handler = logging.StreamHandler()

log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(log_format)
console_handler.setFormatter(log_format)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


pd.set_option("future.no_silent_downcasting", True)

service_name = "zoho_mail"
username = "samplemail@finnable.com"
today_date = datetime.now().strftime("%d-%b-%Y")
email_address = "samplemail@finnable.com"
smtp_server = "smtppro.zoho.com"
smtp_port = 465

master_payment_df = pd.read_feather(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\1. Tech Generated Files\2. Daily Payment Reports\payment_sheet - feather.feather")
master_payment_df["payment_date"] = pd.to_datetime(master_payment_df["payment_date"], format="mixed")

yesterday = pd.to_datetime(today_date) - timedelta(days=1)
yesterday_payment_rows = master_payment_df[master_payment_df["payment_date"].dt.date == yesterday.date()]
if len(yesterday_payment_rows) > 200:
    logger.info("🟢 new payments found proceeding further..")
else:
    logger.info("🔴 No new payments found exiting..")
    sys.exit()

export_base_folder = r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\8. All Lender\2. Nbfc Collection Update MIS"
master_hdb_mis_sheet_path = r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\1. HDB\1. UTR Breakup Files\hdb_master_snappy.parquet"
master_dmi_mis_sheet_path = r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\3. DMI\8. BBPS Payments FIle\bbps_master_dmi_snappy.parquet"
pos_df = pd.read_parquet(
    r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\1. Tech Generated Files\5. POS Files\master_pos_file_snappy.parquet",
    columns=["Loan Account No", "EMI_Amount"],)

utkarsh_portfolio_file = (
    pd.read_parquet(
        r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\3. Lender Received Files\3. UTKARSH\2. Portfolio Files\Utkarsh_Portfolio_25-Nov-24-snappy.parquet",
        columns=["SANCTIONED_REFERENCE_NO", "ACCOUNT_NUMBER"]
    )
    .rename(columns={"SANCTIONED_REFERENCE_NO": "app_id", "ACCOUNT_NUMBER": "lender_loan_ref_id"})
)
utkarsh_portfolio_file["lender_loan_ref_id"] = utkarsh_portfolio_file["lender_loan_ref_id"].astype(str)

master_mis_df_hdb = pd.read_parquet(master_hdb_mis_sheet_path)
master_mis_df_dmi = pd.read_parquet(master_dmi_mis_sheet_path)
thirty_days_ago = pd.to_datetime("today") - pd.Timedelta(days=30)
current_month_df = master_payment_df[master_payment_df["payment_date"] >= thirty_days_ago].astype(str)
export_file_df = {}


def hdb_file(current_month_df, master_mis_df_hdb):
    nbfc = "HDB"
    sort_order = [
    'app_id', 'lender_loan_ref_id', 'nbfc', 'customer_name', 'transaction_id',
    'payment_collected', 'principal_paid', 'interest_paid', 'bounce_charge_paid',
    'penal_charge_paid', 'closure_charges_paid', 'suspense_amount', 'payment_date',
    'payment_month', 'settlement_utr', 'settlement_date', 'order_id', 'source',
    'remarks', 'payment_type', 'current_bucket', 'pos', 'match'
]
    try:
        payment_hdb_df = current_month_df[current_month_df["nbfc"].isin(["HDB", "HDB-Direct"])].copy() 
        payment_hdb_df.drop(columns=['finnable_npa', 'settlement_status', 'mode', 'link_source', 'sub_mode', 'generated_by'], inplace=True) 
        payment_hdb_df.set_index("transaction_id", inplace=True)
        master_mis_df_hdb.set_index("transaction_id", inplace=True)
        payment_with_utr = payment_hdb_df[payment_hdb_df["settlement_utr"] != "-"]

        logger.info("Filtering new transactions:")
        new_trans = payment_with_utr[~payment_with_utr.index.isin(master_mis_df_hdb.index)]

        logger.info("🔴 Payments with null UTR:")
        payment_with_no_utr = payment_hdb_df[payment_hdb_df["settlement_utr"] == "-"].groupby(["payment_date", "source"]).size().reset_index(name="count")
        payment_with_no_utr["payment_date"] = pd.to_datetime(payment_with_no_utr["payment_date"], format="mixed")
        payment_with_no_utr["days_count"] = pd.to_datetime(datetime.now().date()) - payment_with_no_utr["payment_date"]
        logger.info(f"\n{payment_with_no_utr}\n")

        logger.info("Payment with False match")
        logger.info(len(new_trans[new_trans['match'] == "False"]))
        new_trans = new_trans.reset_index()
        new_trans = new_trans[sort_order]
        if new_trans is None:
            logger.info("🔴 No new transactions found hence not exporting 😿")
        else:
            export_path = os.path.join(export_base_folder, rf"{nbfc}\{nbfc} Collection Update {datetime.now().strftime("%b-%y")}.xlsx")
            new_trans.to_excel(export_path, index=False, sheet_name=nbfc)
            logger.info("🟢 HDB File Created 😁")
    except Exception as e:
        return logger.error(f"🔴 cannot process hdb file: {e} 😿")

def other_file(nbfc, current_month_df):
    try:
        if nbfc == "TVS":
            filter = ['TVS', 'TVS-Online']
        else:
            filter = [nbfc]
        nbfc_df = current_month_df[current_month_df["nbfc"].isin(filter)].copy()
        nbfc_df = nbfc_df.drop(columns=['mode', 'link_source','sub_mode', 'generated_by'])
        
        if nbfc == "Utkarsh":
            lender_loan_ref_map = utkarsh_portfolio_file.set_index('app_id')['lender_loan_ref_id'].to_dict()
            nbfc_df.loc[:, 'lender_loan_ref_id'] = nbfc_df['app_id'].map(lender_loan_ref_map)
            nbfc_df.loc[:, 'lender_loan_ref_id'] = nbfc_df['lender_loan_ref_id'].fillna('Not Found')
            
        export_path = os.path.join(export_base_folder, rf"{nbfc}\{nbfc} Collection Update {datetime.now().strftime('%b-%y')}.xlsx")
        nbfc_df.to_excel(export_path, index=False, sheet_name=nbfc)
        return (logger.info(f"🟢 {nbfc} File Created & exported 😁"))
    except Exception as e:
        return (logger.error(f"🔴 cannot process {nbfc} file: {e} 😿"))

def dmi_file(current_month_df, master_mis_df_dmi):
    nbfc = 'DMI'
    try:
        payment_dmi_df = current_month_df[current_month_df['nbfc'].isin(['DMI'])]
        payment_dmi_df = payment_dmi_df[(payment_dmi_df['source'].isin(['BBPS'])) & (payment_dmi_df['settlement_utr'] != '-')]
        payment_dmi_df = payment_dmi_df.drop(columns=['mode', 'link_source','sub_mode', 'generated_by'])


        payment_dmi_df = payment_dmi_df.rename(columns={
            'app_id': 'Application ID', 
            'customer_name': 'Borrower Name', 
            'payment_collected': 'Received Amt', 
            'payment_date': 'Payment Collected Date',
            'transaction_id': 'Unique Token No', 
            'settlement_utr': 'Reference', 
            'settlement_date': 'SOA/Bank Date',
            'payment_type': 'Payment Type',
            'penal_charge_paid': 'Penal Charge Paid',
            'bounce_charge_paid': 'Bounce Charge Paid',
            'match': 'Match'
        })
        
        payment_dmi_df[['Ticket No.', 'Date', 'Channel Partner', 'Accrued Interest/Interest', 'Penal Interest/Overdue Interest','Closure Charges', 'Charge Type', 'Status', 'Bounce Reason', 'UMRN', 'Mode of Payment', 'Source', 'Remarks']] = pd.DataFrame({
            'Ticket No.': '-',
            'Date': datetime.today().strftime('%Y-%m-%d'),
            'Channel Partner': 'Finnable',
            'Accrued Interest/Interest': '-',
            'Penal Interest/Overdue Interest': '-',
            'Closure Charges': 0,
            'Charge Type': '-',
            'Status': 'Clear',
            'Bounce Reason': '-',
            'UMRN': '',
            'Mode of Payment': '-',
            'Source': 'BBPS',
            'Remarks': '-'
        },index=payment_dmi_df.index)
        
        logger.info('Columns Added & Renamed now calculating difference 🙌')
        payment_dmi_df['Difference'] = payment_dmi_df['Received Amt'].astype('float') - payment_dmi_df['Bounce Charge Paid'].astype('float') - payment_dmi_df['Penal Charge Paid'].astype('float') - payment_dmi_df['principal_paid'].astype('float') - payment_dmi_df['interest_paid'].astype('float')
        
        columns_to_copy = ['Application ID', 'Borrower Name',
            'Unique Token No', 'Received Amt', 'Bounce Charge Paid', 'Penal Charge Paid', 'closure_charges_paid', 'Payment Collected Date', 'payment_month',
            'Reference', 'SOA/Bank Date', 'Payment Type', 'Match', 'Ticket No.', 'Date', 'Channel Partner',
            'Accrued Interest/Interest', 'Penal Interest/Overdue Interest',
            'Closure Charges', 'Charge Type', 'Status', 'Bounce Reason', 'UMRN',
            'Mode of Payment', 'Source', 'Remarks', 'Difference']

        payment_dmi_df = payment_dmi_df[columns_to_copy].copy()
        logger.info('Calculating No. of EMI now 🙌')
        emi_mapping = pos_df.set_index('Loan Account No')['EMI_Amount'].to_dict()
        payment_dmi_df['Actual EMI'] = payment_dmi_df['Application ID'].map(emi_mapping).fillna(0).astype('float')
        payment_dmi_df['Received Amt'] = payment_dmi_df['Received Amt'].astype('float')
        # Modified EMI calculation to avoid warnings
        emi_calc = payment_dmi_df['Received Amt'].div(payment_dmi_df['Actual EMI'])
        emi_calc = emi_calc.mask(emi_calc.isin([np.inf, -np.inf]), np.nan)
        payment_dmi_df['# Of EMI'] = emi_calc.fillna(0).round(0).astype('int64')

        # Define sort order
        sort_order = [
            "Date", "Application ID", "Channel Partner", "Borrower Name", "Payment Collected Date",
            "SOA/Bank Date", "Received Amt", "Actual EMI", "Accrued Interest/Interest",
            "Penal Interest/Overdue Interest", "Bounce Charge Paid", "Penal Charge Paid",
            "Closure Charges", "Charge Type", "Payment Type", "# Of EMI", "Difference",
            "Status", "Bounce Reason", "Reference", "UMRN", "Mode of Payment", "Source",
            "Unique Token No", "Remarks"
        ]

        # Modified fillna operations to avoid warnings
        payment_dmi_df['Bounce Charge Paid'] = pd.to_numeric(payment_dmi_df['Bounce Charge Paid'], errors='coerce').fillna(0)
        payment_dmi_df['Penal Charge Paid'] = pd.to_numeric(payment_dmi_df['Penal Charge Paid'], errors='coerce').fillna(0)

        payment_dmi_df.columns

        payment_dmi_df.set_index('Unique Token No', inplace=True)
        master_mis_df_dmi.set_index('Unique Token No', inplace=True)
        logger.info('Filtering new transactions now 🙌')
        new_trans = payment_dmi_df[~payment_dmi_df.index.isin(master_mis_df_dmi.index)]
        new_trans = new_trans.reset_index()
        new_trans = new_trans[sort_order]

        
        export_path = os.path.join(export_base_folder, rf"{nbfc}\{nbfc} Collection Update {datetime.now().strftime("%b-%y")}.xlsx")
        new_trans.to_excel(export_path, index=False, sheet_name=nbfc)
        return (logger.info(f"🟢 {nbfc} File Created & exported 😁"))
    except Exception:
        return (logger.error(f"🔴 cannot process {nbfc} file: {traceback.format_exc()} 😿"))

def create_pivot_table(nbfc):
    try:
        attachment_file_path = pd.read_excel(os.path.join(export_base_folder, rf"{nbfc}\{nbfc} Collection Update {datetime.now().strftime('%b-%y')}.xlsx"), engine='calamine')
        if nbfc == 'DMI':
            rename_dict = {
                'SOA/Bank Date': 'settlement_date',
                'Received Amt': 'payment_collected',
                'Reference': 'settlement_utr',
                'Source': 'source'
            }
            attachment_file_path.rename(columns=rename_dict, inplace=True)
        export_file_df[nbfc] = attachment_file_path
        
        pivot_t = attachment_file_path.pivot_table(index=['settlement_utr', 'settlement_date', 'source'], values='payment_collected', aggfunc='sum')
        pivot_t.sort_values(by = 'settlement_date', ascending=False, inplace=True)
        pivot_t = pivot_t.reset_index()
        # pivot_html = pivot_t.head(20).to_html(index = False)
        logger.info(f"🟢 Sample pivot table for {nbfc} 🙌")
        return(pivot_t.head(20))
        # return pivot_html
    except Exception:
        return logger.error(f"🔴 Error creating pivot table for {nbfc}: {traceback.format_exc()} 🙌")

def create_html_pivot(df_table):
    try:
        html_df_table = df_table.to_html(index=False)
        logger.info("🟢 html pivot created 🙌")
        return html_df_table
    except Exception as e:
        logger.error(f"🔴 Error creating html pivot table: {df_table, e} 🙌")

def update_master_df(nbfc, master_df):
    try:
        logger.info(f"🔃 Updating master file for {nbfc}")
        if nbfc == 'DMI':           
            export_dmi_df = export_file_df[nbfc]

            reversed_dict = {
            'settlement_date': 'SOA/Bank Date',
            'payment_collected': 'Received Amt',
            'settlement_utr': 'Reference',
            'source': 'Source'
            }
            export_dmi_df.rename(columns=reversed_dict, inplace=True)

            export_dmi_df.set_index('Unique Token No', inplace=True)
            
            new_trans = export_dmi_df[~export_dmi_df.index.isin(master_df.index)]
            new_trans.reset_index(inplace=True)
            master_df.reset_index(inplace=True)
            master_df = pd.concat([master_df, new_trans], ignore_index=True).copy()
            master_df = master_df.astype(str)
            master_df.to_parquet(master_dmi_mis_sheet_path, index=False, compression="snappy")
            master_df.to_csv(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\3. DMI\8. BBPS Payments FIle\DMI_Master_File.csv", index=False)
            logger.info(f"🟢 Master {nbfc} File Updated")
            
        elif nbfc == 'HDB':
            # master_df.set_index('transaction_id', inplace=True)
            export_file_df[nbfc].set_index('transaction_id', inplace=True)
            new_trans = export_file_df[nbfc][~export_file_df[nbfc].index.isin(master_df.index)]
            new_trans.reset_index(inplace=True)
            master_df.reset_index(inplace=True)
            master_df = pd.concat([master_df, new_trans], ignore_index=True).copy()
            master_df = master_df.astype(str)
            master_df.to_parquet(master_hdb_mis_sheet_path, index=False, compression="snappy")
            master_df.to_csv(r"C:\Users\Arc\Downloads\OneDrive\OneDrive - zp3bp\4. Working Files\1. HDB\1. UTR Breakup Files\HDB_Master_File.csv", index=False)
            logger.info(f"🟢 Master {nbfc} File Updated")
    except Exception as e:
        return logger.error(f"🔴 Error updating master file for {nbfc}: {e} 🙌")

issue_sending_mail = False

def send_email(partner_name, to, cc, name, pivot_html, attachment_path):
    global issue_sending_mail
    # Prepare email body with dynamic content for name and pivot_html
    email_body = body_template.format(name=name, pivot_html=pivot_html)

    # Prepare the subject with partner name and current date
    subject = f"{partner_name} Collection Update {datetime.now().date()}"

    # Create message container
    msg = MIMEMultipart()
    msg['From'] = email_address
    msg['To'] = ", ".join(to)
    msg['CC'] = ", ".join(cc + universal_cc)  # Add universal CC
    msg['Subject'] = subject

    # Attach the HTML body
    msg.attach(MIMEText(email_body, 'html'))

    # Attach the file if path exists
    if attachment_path and os.path.exists(attachment_path):
        # Open the file in binary mode
        with open(attachment_path, 'rb') as attachment_file:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment_file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment_path)}')
            msg.attach(part)

    # Loop to handle login retries on authentication error
    while True:
        try:
            logger.info(f"Attempting to connect to SMTP server for {partner_name}...")
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                password = keyring.get_password(service_name, username)

                logger.info("Connected. Attempting to log in...")
                server.login(email_address, password)
                logger.info("Logged in. Sending the email...")

                server.sendmail(email_address, to + cc + universal_cc, msg.as_string())  # Send to, cc, and universal cc
                logger.info(f"Email sent to {name} ({', '.join(to)})")
                break  # Exit after sending the email successfully

        except SMTPAuthenticationError:
            logger.error("Error: Incorrect password. Please try again.")
            password = keyring.get_password(service_name, username)
            issue_sending_mail = True
            continue  # Retry the connection
        except SMTPConnectError:
            logger.error("Error: Could not connect to the SMTP server. Please check your internet connection and SMTP server address.")
            issue_sending_mail = True
            break  # Exit after this error
        except socket.gaierror:
            logger.error("Error: Network issue. Unable to resolve the SMTP server address.")
            issue_sending_mail = True
            break  # Exit after this error
        except SMTPRecipientsRefused:
            logger.error("Error: One or more recipient's email address was refused. Please verify the recipient's address.")
            issue_sending_mail = True
            break  # Exit after this error
        except SMTPServerDisconnected as e:
            logger.error(f"Error: SMTP server disconnected unexpectedly: {e}")
            issue_sending_mail = True
            break  # Exit after this error
        except KeyboardInterrupt:
            logger.error("\nProcess interrupted by user. Exiting...")
            issue_sending_mail = True
            sys.exit()  # Gracefully exit if the user presses Ctrl+C
        except SMTPException as e:
            logger.error(f"SMTP Error: {e}")
            issue_sending_mail = True
            break  # Exit after this error
        except Exception as e:  # Catch any other unexpected exceptions
            logger.error(f"An unexpected error occurred: {e}")
            issue_sending_mail = True
            break  # Exit after this error

body_template = """
<html>
<body>
    <div>Hi {name},</div><br>
    <div>Kindly find the collection posting file. Request you to ignore the UTR which are already posted. Further, let me know in any case of discrepancy.</div>
    <div>Kindly find the Summary of last 20 settlements:</div><br>
    {pivot_html}
    <br>
    <div>Thanks & Regards,</div>
    <div style="color: #01437c;">Shivam Ashiwal</div>
    <div><strong>Finance Analyst | Finance</strong></div>
</body>
</html>
"""

mail_dict = {
    'Piramal': {
        'to': [''],
        'cc': ['' ],
        'name': 'Kunal',
    } # more recepeints can be added later on
}
    
universal_cc = ['']


list_of_partners = [
    # 'Gosree',
    ]


if not list_of_partners:
    logger.info("🔴 No Partners selected. Exiting...")
    sys.exit()
else:
    for partner in list_of_partners:
        if partner == 'HDB':
            hdb_file(current_month_df, master_mis_df_hdb)
            logger.info(create_pivot_table(partner))
        elif partner == 'DMI':
            dmi_file(current_month_df, master_mis_df_dmi)
            logger.info(create_pivot_table(partner))
        else:
            other_file(partner, current_month_df)
            logger.info(create_pivot_table(partner))
    

    for partner in list_of_partners:
        send_email(
            partner_name=partner,
            to=mail_dict[partner]['to'],
            cc=mail_dict[partner]['cc'],
            name=mail_dict[partner]['name'],
            pivot_html=create_html_pivot(create_pivot_table(partner)),
            attachment_path=(os.path.join(export_base_folder, rf"{partner}\{partner} Collection Update {datetime.now().strftime('%b-%y')}.xlsx"))
        )
        if not issue_sending_mail:
            if partner == 'HDB':
                update_master_df('HDB', master_mis_df_hdb)
            elif partner == 'DMI':
                update_master_df('DMI', master_mis_df_dmi)

logger.info("✅ All done! Exiting...")
print("✅ All done! Exiting...")
