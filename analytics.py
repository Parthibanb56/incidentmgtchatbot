from db import run_query

def get_status_summary():
    return run_query("""
        SELECT ticketstatus, COUNT(*) total
        FROM insurance.ticketdetails
        GROUP BY ticketstatus
    """)

def get_monthly_trend():
    return run_query("""
        SELECT MONTH(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y')) month, COUNT(*) total
        FROM insurance.ticketdetails
        GROUP BY MONTH(STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y'))
    """)

def get_overdue_cases():
    return run_query("""
        SELECT COUNT(*) total
        FROM insurance.ticketdetails
        WHERE ticketstatus='Pending'
        AND STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y') < CURDATE() - INTERVAL 7 DAY
    """)