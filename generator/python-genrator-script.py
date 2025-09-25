import json
import random
import uuid
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Event Hub Configuration
EVENTHUBS_NAMESPACE = "analytics-adarsh.servicebus.windows.net"
EVENT_HUB_NAME = "court-proceedings-eh"
CONNECTION_STRING = "Endpoint=sb://analytics-adarsh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ApHQX2K/6b/5MZkRUCSNPFpsawozxbdLc+AEhAOzmBs="

producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],     
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Reference data
charge_types = ["Theft", "Assault", "Drug Possession", "DUI", "Burglary", "Fraud", "Traffic Violation", "Domestic Violence"]
charge_severities = ["Violation", "Misdemeanor", "Felony"]
attorney_statuses = ["Private Attorney", "Public Defender Assigned", "Public Defender Requested", "Self-Represented", "Attorney Pending"]
income_levels = ["Low", "Medium", "High"]
court_districts = ["Manhattan Criminal Court", "Brooklyn Criminal Court", "Queens Criminal Court", "Bronx Criminal Court", "Staten Island Criminal Court"]
case_statuses = ["Filed", "Pending", "Active", "Scheduled"]
genders = ["Male", "Female", "Other"]
hearing_types = ["Arraignment", "Pre-trial", "Plea Hearing", "Trial", "Sentencing"]
judges = ["Judge Martinez", "Judge Johnson", "Judge Williams", "Judge Brown", "Judge Davis"]

# NYC zip codes
zip_codes = [10001, 10002, 10003, 11201, 11205, 11215, 11101, 11103, 10451, 10452, 10301, 10302]

def inject_dirty_data(record):
    # 3% chance of missing zip code
    if random.random() < 0.03:
        record["defendant_zip_code"] = None
    # 2% chance of future hearing date
    if random.random() < 0.02:
        record["hearing_date"] = (datetime.utcnow() + timedelta(days=random.randint(60, 120))).isoformat()
    return record

def generate_case_id():
    year = datetime.now().year
    district_code = random.choice(["MN", "BK", "QN", "BX", "SI"])
    case_num = random.randint(100000, 999999)
    return f"{year}-{district_code}-{case_num:06d}"

def generate_court_event():
    case_filed_date = datetime.utcnow() - timedelta(days=random.randint(1, 90))
    hearing_date = case_filed_date + timedelta(days=random.randint(7, 45))
    
    event = {
        "case_id": generate_case_id(),
        "defendant_id": f"DEF-{random.randint(100000, 999999)}",
        "charge_type": random.choice(charge_types),
        "charge_severity": random.choice(charge_severities),
        "case_status": random.choice(case_statuses),
        "attorney_status": random.choice(attorney_statuses),
        "defendant_age": random.randint(18, 75),
        "defendant_gender": random.choice(genders),
        "income_level": random.choice(income_levels),
        "defendant_zip_code": random.choice(zip_codes),
        "court_district": random.choice(court_districts),
        "assigned_judge": random.choice(judges),
        "hearing_type": random.choice(hearing_types),
        "case_filed_date": case_filed_date.isoformat(),
        "hearing_date": hearing_date.isoformat(),
        "record_timestamp": datetime.utcnow().isoformat()
    }
    
    return inject_dirty_data(event)

if __name__ == "__main__":
    print("Starting Court Data Generator...")
    print(f"Sending data to Event Hub: {EVENT_HUB_NAME}")
    print("Press Ctrl+C to stop\n")
    
    record_count = 0
    
    try:
        while True:
            event = generate_court_event()
            producer.send(EVENT_HUB_NAME, event)
            record_count += 1
            
            print(f"Sent record #{record_count}: {event['case_id']}")
            
            if record_count % 25 == 0:
                print(f"\n--- {record_count} records sent ---\n")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\nStopped. Total records: {record_count}")
        producer.close()
    except Exception as e:
        print(f"Error: {str(e)}")
        producer.close()
