from datetime import datetime

respfit_neshap_columns = {
    "ADP_NUMBER": str,
    "WORKDAY_NUMBER": str,
    "STATUS": str,
    "REGION": str,
    "ZONE": str,
    "LAST_NAME": str,
    "FIRST_NAME": str,
    "FITTEST_COMPLIANT": str,
    "NESHAP_COMPLIANT": str,
    #"ACCOUNT_PERIOD": datetime
}

hazcom_ppe_columns = {
    "ADP_NUMBER": str,
    "WORKDAY_NUMBER": str,
    "STATUS": str,
    "REGION": str,
    "ZONE": str,
    "LAST_NAME": str,
    "FIRST_NAME": str,
    "HAZCOM_COMPLIANT": str,
    "PPE_COMPLIANT": str,
    #"ACCOUNT_PERIOD": datetime
}