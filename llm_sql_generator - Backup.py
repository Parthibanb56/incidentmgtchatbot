import ollama
import re
from config import LLM_MODEL

SCHEMA = """
Table: ticketdetails

Columns:
- title (varchar)
- tat (int)
- incidentid (varchar)
- ticketstatus (varchar)
- ticketsubmitteddatetime (datetime)
- ticketcloseddatetime (datetime)
- reqname (varchar)
- reqemail (varchar)
- typeofticket (varchar)
- category (varchar)
- priorityseverity (varchar)
- product (varchar)
- reportedby (varchar)
- subcategory (varchar)
- subsubcategory (varchar)
- assignperson (varchar)
- assignpersonemail (varchar)
- assigngroup (varchar)
"""

def clean_sql(text: str) -> str:
    """
    Extract SQL from LLM response and clean formatting.
    """

    # remove markdown
    text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE).strip()

    # remove explanations before SELECT
    select_index = text.lower().find("select")
    if select_index != -1:
        text = text[select_index:]

    # remove trailing semicolons & spaces
    text = text.strip().rstrip(";")

    return text


def generate_sql(question: str) -> str | None:
    """
    - Generate SAFE SELECT SQL from user question.
    - Output MUST be a single SELECT statement.
    - Must include LIMIT 50 (unless aggregate).
    - MUST NOT modify data (INSERT/UPDATE/DELETE/DDL).
    - Return ONLY SQL (no prose).
    """
    prompt = f"""
You are an expert MySQL query generator.

Convert the user question into a SAFE SQL query.

IMPORTANT RULES:
- Output exactly one SQL statement.
- SELECT only. Do not modify data.
- Use LIMIT 50 for non-aggregations.
- Prefer IS NOT NULL over != '' when checking presence.
- Use DATE/DATETIME functions when the question uses ranges like "today", "yesterday", "last 7 days".
- RETURN ONLY SQL (no explanations).

Database Schema:
{SCHEMA}

Examples:

User: how many pending tickets
SQL:
SELECT COUNT(*) AS totalpending
FROM ticketdetails
WHERE ticketstatus <> 'Closed';

User: how many new tickets
SQL:
SELECT COUNT(*) AS totalpending
FROM ticketdetails
WHERE ticketstatus LIKE '%New%';

User: show the new tickets
SQL:
SELECT COUNT(*) AS totalpending
FROM ticketdetails
WHERE ticketstatus LIKE '%New%';

User: status of incident id INC-002
SQL:
SELECT ticketstatus
FROM ticketdetails
WHERE incidentid = 'INC-002'
LIMIT 50;

User: show tickets assigned to parthiban
SQL:
SELECT *
FROM ticketdetails
WHERE assignperson LIKE '%parthiban%'
LIMIT 50;

User: how many High tickets
SQL:
SELECT COUNT(*) AS totalp1
FROM ticketdetails
WHERE priorityseverity LIKE '%High%';

User: how many S1 tickets
SQL:
SELECT COUNT(*) AS totalp1
FROM ticketdetails
WHERE priorityseverity LIKE '%S1%';

User: how many S2: tickets
SQL:
SELECT COUNT(*) AS totalp1
FROM ticketdetails
WHERE priorityseverity LIKE '%S2:%';

User: list tickets opened today
SQL:
SELECT *
FROM ticket_details
WHERE DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) = CURRENT_DATE()
LIMIT 50;

User: tickets closed in last 7 days
SQL:
SELECT *
FROM ticketdetails
WHERE DATE(STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i')) >= DATESUB(NOW(), INTERVAL 7 DAY)
LIMIT 50;

User: average TAT for closed tickets
SQL:
SELECT AVG(tat) AS avgtat
FROM ticketdetails
WHERE DATE(STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i')) IS NOT NULL;

User: tickets under product core
SQL:
SELECT *
FROM ticketdetails
WHERE product LIKE '%core%'
LIMIT 50;

User: submitted between 01/02/2026 and 21/02/2026
SQL:
SELECT *
FROM ticketdetails
WHERE DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) >= STRTODATE('01/02/2026', '%d/%m/%Y')
  AND DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) <  DATEADD(STRTODATE('21/02/2026', '%d/%m/%Y'), INTERVAL 1 DAY)
LIMIT 50;

User: show incidents with email like @contoso.com
SQL:
SELECT incidentid, reqemail, ticketstatus
FROM ticketdetails
WHERE reqemail LIKE '%@contoso.com%'
LIMIT 50;

User: top 5 assignees by open tickets
SQL:
SELECT assignperson, COUNT(*) AS totalopen
FROM ticketdetails
WHERE ticketstatus IN ('New', 'Pending', 'In Progress')
GROUP BY assignperson
ORDER BY totalopen DESC
LIMIT 5;

User: tickets with TAT > 120
SQL:
SELECT *
FROM ticketdetails
WHERE tat > 120
LIMIT 50;

User: breakdown by category for last month
SQL:
SELECT category, COUNT(*) AS total
FROM ticketdetails
WHERE DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) >= DATESUB(DATESUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE())-1 DAY), INTERVAL 1 MONTH)
  AND DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) <  DATESUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE())-1 DAY)
GROUP BY category
ORDER BY total DESC
LIMIT 50;

User: latest 50 tickets
SQL:
SELECT *
FROM ticketdetails
ORDER BY DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) DESC
LIMIT 50;

User: open Low network issues assigned to Level1
SQL:
SELECT *
FROM ticketdetails
WHERE ticketstatus IN ('Open', 'In Progress', 'Pending')
  AND priorityseverity LIKE '%Low%'
  AND (subcategory = 'Network' OR subcategory LIKE '%Network%')
  AND assigngroup = 'Level1'
LIMIT 50;

User: tickets without assignee
SQL:
SELECT *
FROM ticketdetails
WHERE assignperson IS NULL OR assignperson = ''
LIMIT 50;

User: req emails missing domain
SQL:
SELECT incidentid, reqemail
FROM ticketdetails
WHERE reqemail NOT LIKE '%@%' OR reqemail IS NULL OR reqemail = ''
LIMIT 50;

User: tickets closed this week
SQL:
SELECT *
FROM ticketdetails
WHERE YEARWEEK(DATE(STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i')), 1) = YEARWEEK(CURDATE(), 1)
LIMIT 50;

User: average TAT by priority
SQL:
SELECT priorityseverity, AVG(tat) AS avgtat
FROM ticketdetails
WHERE tat IS NOT NULL
GROUP BY priorityseverity
ORDER BY avgtat DESC
LIMIT 50;

User: count by assigngroup
SQL:
SELECT assigngroup, COUNT(*) AS total
FROM ticketdetails
GROUP BY assigngroup
ORDER BY total DESC
LIMIT 50;

User: incidents created yesterday
SQL:
SELECT *
FROM ticketdetails
WHERE DATE(DATE(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i'))) = DATESUB(CURRENTDATE(), INTERVAL 1 DAY)
LIMIT 50;

User: berapa banyak tiket low
SQL:
SELECT COUNT(*) AS totalp1
FROM ticketdetails
WHERE priorityseverity LIKE '%Low%';

User: senaraikan tiket di bawah kategori Infra
SQL:
SELECT *
FROM ticketdetails
WHERE category LIKE '%Infra%'
LIMIT 50;

User: tiket dibuka semalam
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') = DATESUB(CURRENTDATE(), INTERVAL 1 DAY)
LIMIT 50;

User: tiket di bawah kumpulan Level2
SQL:
SELECT *
FROM ticketdetails
WHERE assigngroup LIKE '%Level2%'
LIMIT 50;

User: jumlah tiket melebihi TAT 60
SQL:
SELECT COUNT(*) AS totaltatover60
FROM ticketdetails
WHERE tat > 60;

User: tiket dilaporkan oleh Ali
SQL:
SELECT *
FROM ticketdetails
WHERE reportedby LIKE '%Ali%'
LIMIT 50;

User: tiket terbaru untuk S1
SQL:
SELECT *
FROM ticketdetails
WHERE priorityseverity LIKE '%S1%'
ORDER BY STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') DESC
LIMIT 50;

User: tiket ditutup minggu ini untuk produk Core
SQL:
SELECT *
FROM ticketdetails
WHERE YEARWEEK(STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i'), 1) = YEARWEEK(CURDATE(), 1)
  AND product LIKE '%Core%'
LIMIT 50;

User: jumlah tiket terbuka ikut kumpulan tugasan
SQL:
SELECT assigngroup, COUNT(*) AS totalopen
FROM ticketdetails
WHERE ticketstatus IN ('Open', 'Pending', 'In Progress')
GROUP BY assigngroup
ORDER BY totalopen DESC
LIMIT 50;

User: tiket yang dihantar minggu lepas
SQL:
SELECT *
FROM ticketdetails
WHERE YEARWEEK(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i'), 1) = YEARWEEK(DATESUB(CURDATE(), INTERVAL 7 DAY), 1)
LIMIT 50;

User: berapa jumlah tiket berstatus Pending
SQL:
SELECT COUNT(*) AS jumlahpending
FROM ticketdetails
WHERE ticketstatus <> 'Closed';

User: status untuk insiden INC-010
SQL:
SELECT ticketstatus
FROM ticketdetails
WHERE incidentid = 'INC-010'
LIMIT 50;

User: senaraikan tiket yang ditugaskan kepada Farid
SQL:
SELECT *
FROM ticketdetails
WHERE assignperson LIKE '%Farid%'
LIMIT 50;

User: berapa banyak tiket P2 minggu ini
SQL:
SELECT COUNT(*) AS jumlahp2mingguini
FROM ticketdetails
WHERE priorityseverity LIKE '%P2%'
  AND YEARWEEK(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i'), 1) = YEARWEEK(CURDATE(), 1);

User: tiket yang dibuka hari ini
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') = CURRENTDATE()
LIMIT 50;

User: tiket yang ditutup dalam 7 hari lepas
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i') >= DATESUB(NOW(), INTERVAL 7 DAY)
LIMIT 50;

User: tiket dihantar antara 01/02/2026 dan 21/02/2026
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') >= STRTODATE('01/02/2026', '%d/%m/%Y')
  AND STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') <  DATEADD(STRTODATE('21/02/2026', '%d/%m/%Y'), INTERVAL 1 DAY)
LIMIT 50;

User: purata TAT untuk tiket yang sudah ditutup
SQL:
SELECT AVG(tat) AS puratatat
FROM ticketdetails
WHERE STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i') IS NOT NULL;

User: senarai tiket untuk produk mengandungi perkataan Core
SQL:
SELECT *
FROM ticketdetails
WHERE product LIKE '%Core%'
LIMIT 50;

User: insiden dengan email permohon domain @company.com
SQL:
SELECT incidentid, reqemail, ticketstatus
FROM ticketdetails
WHERE reqemail LIKE '%@company.com%'
LIMIT 50;

User: 5 penerima tugasan dengan tiket terbuka terbanyak
SQL:
SELECT assignperson, COUNT(*) AS jumlahterbuka
FROM ticketdetails
WHERE ticketstatus IN ('Open', 'Pending', 'In Progress')
GROUP BY assignperson
ORDER BY jumlahterbuka DESC
LIMIT 5;

User: tiket dengan TAT melebihi 120 minit
SQL:
SELECT *
FROM ticketdetails
WHERE tat > 120
LIMIT 50;

User: pecahan mengikut kategori untuk bulan lepas
SQL:
SELECT category, COUNT(*) AS jumlah
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') >= DATESUB(DATESUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE())-1 DAY), INTERVAL 1 MONTH)
  AND STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') <  DATESUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE())-1 DAY)
GROUP BY category
ORDER BY jumlah DESC
LIMIT 50;

User: 50 tiket terbaru
SQL:
SELECT *
FROM ticketdetails
ORDER BY STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') DESC
LIMIT 50;

User: isu P1 rangkaian di bawah kumpulan Level1
SQL:
SELECT *
FROM ticketdetails
WHERE ticketstatus IN ('Open', 'In Progress', 'Pending')
  AND priorityseverity Like '%P1%'
  AND (subcategory = 'Network' OR subcategory LIKE '%Network%')
  AND assigngroup = 'Level1'
LIMIT 50;

User: tiket tanpa penerima tugasan
SQL:
SELECT *
FROM ticketdetails
WHERE assignperson IS NULL OR assignperson = ''
LIMIT 50;

User: tiket tanpa email pemohon
SQL:
SELECT *
FROM ticketdetails
WHERE reqemail IS NULL OR reqemail = ''
LIMIT 50;

User: tiket ditutup minggu ini
SQL:
SELECT *
FROM ticketdetails
WHERE YEARWEEK(STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i'), 1) = YEARWEEK(CURDATE(), 1)
LIMIT 50;

User: purata TAT mengikut keutamaan
SQL:
SELECT priorityseverity, AVG(tat) AS puratatat
FROM ticketdetails
WHERE tat IS NOT NULL
GROUP BY priorityseverity
ORDER BY puratatat DESC
LIMIT 50;

User:  jumlah tiket mengikut kumpulan tugasan
SQL:
SELECT assigngroup, COUNT(*) AS jumlah
FROM ticketdetails
GROUP BY assigngroup
ORDER BY jumlah DESC
LIMIT 50;

User: insiden dicipta semalam
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') = DATESUB(CURRENTDATE(), INTERVAL 1 DAY)
LIMIT 50;

User: senaraikan tiket status Open atau In Progress
SQL:
SELECT *
FROM ticketdetails
WHERE ticketstatus IN ('Open', 'In Progress')
LIMIT 50;

User: email permohon bukan dari domain korporat
SQL:
SELECT incidentid, reqemail
FROM ticketdetails
WHERE reqemail NOT LIKE '%@corp.com%'
  AND reqemail IS NOT NULL
  AND reqemail <> ''
LIMIT 50;

User: jumlah tiket setiap produk
SQL:
SELECT product, COUNT(*) AS jumlah
FROM ticketdetails
GROUP BY product
ORDER BY jumlah DESC
LIMIT 50;

User: tiket subkategori Network bulan ini
SQL:
SELECT *
FROM ticketdetails
WHERE subcategory = 'Network'
  AND YEAR(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) = YEAR(CURDATE())
  AND MONTH(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) = MONTH(CURDATE())
LIMIT 50;

User: insiden dilaporkan oleh nama mengandungi “Ali”
SQL:
SELECT *
FROM ticketdetails
WHERE reportedby LIKE '%Ali%'
LIMIT 50;

User: jumlah tiket ditutup semalam
SQL:
SELECT COUNT(*) AS jumlahditutupsemalam
FROM ticketdetails
WHERE STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i') = DATESUB(CURRENTDATE(), INTERVAL 1 DAY);

User:  tiket yang ditugaskan tetapi belum ditutup
SQL:
SELECT *
FROM ticketdetails
WHERE assignperson IS NOT NULL AND assignperson <> ''
  AND STR_TO_DATE(ticketcloseddatetime, '%e/%c/%Y %H:%i') IS NULL
LIMIT 50;

User: 10 tiket paling lama terbuka (ikut tarikh hantar)
SQL:
SELECT *
FROM ticketdetails
WHERE ticketstatus IN ('Open', 'Pending', 'In Progress')
ORDER BY STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') ASC
LIMIT 10;

User: jumlah insiden unik mengikut pelapor (reportedby)
SQL:
SELECT reportedby, COUNT(DISTINCT incidentid) AS jumlahinsiden
FROM ticketdetails
GROUP BY reportedby
ORDER BY jumlahinsiden DESC
LIMIT 50;

User: tiket bertajuk mengandungi “database”
SQL:
SELECT *
FROM ticketdetails
WHERE title LIKE '%database%'
LIMIT 50;

User: tiket kategori Infra tetapi tiada subkategori
SQL:
SELECT *
FROM ticketdetails
WHERE category LIKE'%Infra%'
  AND (subcategory IS NULL OR subcategory = '')
LIMIT 50;

User: senarai insiden dengan domain email @gmail.com
SQL:
SELECT incidentid, reqname, reqemail, ticketstatus
FROM ticketdetails
WHERE reqemail LIKE '%@gmail.com%'
LIMIT 50;

User: tren harian jumlah tiket 14 hari lepas
SQL:
SELECT STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') AS tarikh, COUNT(*) AS jumlah
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i') >= DATESUB(CURDATE(), INTERVAL 14 DAY)
GROUP BY STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')
ORDER BY tarikh ASC
LIMIT 50;

User: tiket diserahkan pada hujung minggu lepas
SQL:
SELECT *
FROM ticketdetails
WHERE WEEKDAY(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i')) IN (5, 6)
  AND YEARWEEK(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y %H:%i'), 1) = YEARWEEK(DATESUB(CURDATE(), INTERVAL 7 DAY), 1)
LIMIT 50;

User Question:
{question}

SQL:
"""

    try:
        response = ollama.chat(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0}
        )

        raw_output = response["message"]["content"]

        sql = clean_sql(raw_output)

        # safety validation
        if not sql.lower().startswith("select"):
            return None

        forbidden = ["update", "delete", "insert", "drop", "alter", "truncate"]
        if any(word in sql.lower() for word in forbidden):
            return None

        return sql

    except Exception as e:
        print("LLM SQL generation error:", e)
        return None