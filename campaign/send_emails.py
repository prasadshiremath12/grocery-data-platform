"""Send personalised recommendation emails from grocery.v_email_campaign_queue.

Dry run (prints, sends nothing):   python -m campaign.send_emails --project my-proj --dry_run --limit 3
Send via SMTP (Gmail app password / SES / SendGrid SMTP):
  SMTP_HOST=smtp.gmail.com SMTP_PORT=587 SMTP_USER=you@shop.com SMTP_PASSWORD=*** \
  python -m campaign.send_emails --project my-proj --sender "Fresh Mart <you@shop.com>"

Logs every send to grocery.email_log so nobody is mailed twice within --cooldown_days.
"""
import argparse
import os
import smtplib
import ssl
import time
from email.message import EmailMessage
from html import escape

from google.cloud import bigquery

QUEUE_SQL = """
SELECT q.* FROM `{p}.grocery.v_email_campaign_queue` q
LEFT JOIN (SELECT customer_id FROM `{p}.grocery.email_log`
           WHERE sent_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {cd} DAY) AND status = 'SENT') l
  USING (customer_id)
WHERE l.customer_id IS NULL
{limit}
"""

SUBJECTS = {
    "Champions": "{name}, a little thank-you from us",
    "Loyal": "{name}, your picks for this week",
    "New / Promising": "{name}, picked just for you",
    "Needs Attention": "{name}, we saved these for you",
    "At Risk (was loyal)": "We miss you, {name} - special prices inside",
    "Lost / Dormant": "{name}, come back for up to {pct}% off",
}


def render(row):
    offers = row["offers"]
    max_pct = max(int(o["discount_pct"]) for o in offers)
    subject = SUBJECTS.get(row["segment"], "{name}, offers for you").format(name=row["name"].split()[0], pct=max_pct)
    items = "".join(
        f"<tr><td style='padding:8px'>{escape(o['product_name'])}<br>"
        f"<small style='color:#666'>{escape(o['reason'])}</small></td>"
        f"<td style='padding:8px;text-align:right'><s style='color:#999'>Rs {o['selling_price']:.0f}</s> "
        f"<b style='color:#0a7d33'>Rs {o['offer_price']:.0f}</b><br><small>{int(o['discount_pct'])}% off</small></td></tr>"
        for o in offers)
    html = f"""<div style='font-family:Arial,sans-serif;max-width:560px;margin:auto'>
<h2>Hi {escape(row['name'].split()[0])},</h2>
<p>{escape(offers[0]['theme'])} - here are items we think you'll love, at special prices:</p>
<table width='100%' style='border-collapse:collapse;border-top:1px solid #eee'>{items}</table>
<p>Show this email at the counter or order on WhatsApp. Offers valid for 7 days.</p>
<p style='font-size:11px;color:#888'>You are receiving this because you opted in to offers. Reply STOP to unsubscribe.</p></div>"""
    text = "\n".join(f"- {o['product_name']}: Rs {o['offer_price']:.0f} ({int(o['discount_pct'])}% off)" for o in offers)
    return subject, text, html


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--sender", default="Fresh Mart <offers@example.com>")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--cooldown_days", type=int, default=7)
    ap.add_argument("--dry_run", action="store_true")
    a = ap.parse_args(argv)

    bq = bigquery.Client(project=a.project)
    bq.query(f"""CREATE TABLE IF NOT EXISTS `{a.project}.grocery.email_log`
                 (customer_id STRING, email STRING, segment STRING, sent_at TIMESTAMP, status STRING, error STRING)""").result()
    rows = list(bq.query(QUEUE_SQL.format(p=a.project, cd=a.cooldown_days,
                                          limit=f"LIMIT {a.limit}" if a.limit else "")).result())
    print(f"{len(rows)} customers in queue")

    smtp = None
    if not a.dry_run:
        smtp = smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ.get("SMTP_PORT", 587)))
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(os.environ["SMTP_USER"], os.environ["SMTP_PASSWORD"])

    log = []
    for r in rows:
        subject, text, html = render(r)
        if a.dry_run:
            print(f"\n--- To: {r['email']} | {subject}\n{text}")
            continue
        msg = EmailMessage()
        msg["From"], msg["To"], msg["Subject"] = a.sender, r["email"], subject
        msg.set_content(text)
        msg.add_alternative(html, subtype="html")
        status, err = "SENT", None
        try:
            smtp.send_message(msg)
        except Exception as e:  # noqa: BLE001
            status, err = "FAILED", str(e)[:300]
        log.append({"customer_id": r["customer_id"], "email": r["email"], "segment": r["segment"],
                    "sent_at": time.strftime("%Y-%m-%d %H:%M:%S"), "status": status, "error": err})
        time.sleep(0.2)  # gentle rate limit

    if log:
        bq.insert_rows_json(f"{a.project}.grocery.email_log", log)
    if smtp:
        smtp.quit()
    print(f"done: {sum(1 for l in log if l['status'] == 'SENT')} sent, {sum(1 for l in log if l['status'] == 'FAILED')} failed")


if __name__ == "__main__":
    main()
