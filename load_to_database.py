import json
import sqlite3
import os
import re
import gc
from datetime import datetime
from collections import defaultdict

def extract_id(obj, key='_id'):
    """Extract MongoDB ObjectId"""
    if not obj or key not in obj:
        return None
    id_val = obj[key]
    if isinstance(id_val, dict) and '$oid' in id_val:
        return id_val['$oid']
    return str(id_val)

def extract_foreign_key(obj, possible_keys):
    """Extract foreign key"""
    if not obj:
        return None
    for key in possible_keys:
        if key in obj:
            id_val = obj[key]
            if isinstance(id_val, dict) and '$oid' in id_val:
                return id_val['$oid']
            return str(id_val) if id_val else None
    return None

def parse_timestamp(ts):
    """Parse timestamp to datetime"""
    if not ts:
        return None
    try:
        if isinstance(ts, dict):
            if '$date' in ts:
                date_val = ts['$date']
                if isinstance(date_val, str):
                    return datetime.fromisoformat(date_val.replace('Z', '+00:00'))
                elif isinstance(date_val, (int, float)):
                    if date_val > 1e12:
                        return datetime.fromtimestamp(date_val / 1000)
                    return datetime.fromtimestamp(date_val)
        elif isinstance(ts, (int, float)):
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1000)
            return datetime.fromtimestamp(ts)
        elif isinstance(ts, str):
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except:
        pass
    return None

def categorize_message(body):
    """Categorize message content"""
    if not body:
        return 'Other'
    
    body_lower = str(body).lower()
    
    categories = {
        'Order/Purchase': ['order', 'طلب', 'اطلب', 'عايز', 'محتاج', 'ابعتلي', 'اوردر', 'شراء', 'اشتري'],
        'Inquiry/Question': ['?', '؟', 'سؤال', 'استفسار', 'ايه', 'how', 'what', 'فين', 'امتى'],
        'Price Inquiry': ['سعر', 'كام', 'بكام', 'price', 'cost', 'تكلفة'],
        'Availability': ['متوفر', 'موجود', 'available', 'عندكم', 'فيه', 'stock'],
        'Alternative Request': ['بديل', 'alternative', 'غيره', 'تاني', 'similar'],
        'Complaint': ['شكوى', 'مشكلة', 'problem', 'issue', 'complaint', 'زعلان', 'غلط'],
        'Delivery': ['توصيل', 'delivery', 'شحن', 'وصل', 'هيوصل', 'العنوان', 'address'],
        'Payment': ['دفع', 'payment', 'فلوس', 'money', 'فيزا', 'كاش', 'تحويل'],
        'Medical Advice': ['دواء', 'علاج', 'medicine', 'treatment', 'ينفع', 'جرعة', 'dose'],
        'Thanks/Feedback': ['شكرا', 'thanks', 'تمام', 'ممتاز', 'great', 'excellent'],
        'Greeting': ['السلام', 'صباح', 'مساء', 'hello', 'hi', 'اهلا', 'ازيك']
    }
    
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in body_lower:
                return category
    
    return 'Other'

def detect_sentiment(body):
    """Detect message sentiment"""
    if not body:
        return 'Neutral'
    
    body_lower = str(body).lower()
    
    positive_words = ['شكرا', 'thanks', 'great', 'excellent', 'ممتاز', 'رائع', 'جميل',
                     'تمام', 'كويس', 'good', 'nice', 'amazing', 'love', 'best', 'perfect',
                     'happy', 'سعيد', 'مبسوط', 'تسلم', 'احسن', 'برافو']
    
    negative_words = ['سيء', 'bad', 'terrible', 'awful', 'worst', 'hate', 'مشكلة',
                     'زعلان', 'غضبان', 'مش كويس', 'وحش', 'problem', 'issue',
                     'شكوى', 'disappointed', 'فاشل', 'متأخر', 'late', 'wrong']
    
    positive_count = sum(1 for w in positive_words if w in body_lower)
    negative_count = sum(1 for w in negative_words if w in body_lower)
    
    if positive_count > negative_count:
        return 'Positive'
    elif negative_count > positive_count:
        return 'Negative'
    return 'Neutral'

def detect_urgency(body):
    """Detect message urgency"""
    if not body:
        return 'Normal'
    
    body_lower = str(body).lower()
    
    urgent = ['urgent', 'ضروري', 'مستعجل', 'بسرعة', 'حالا', 'emergency', 'طوارئ', 'فورا']
    high = ['important', 'مهم', 'please', 'من فضلك', 'محتاج', 'need']
    
    for w in urgent:
        if w in body_lower:
            return 'Urgent'
    for w in high:
        if w in body_lower:
            return 'High'
    
    return 'Normal'

def detect_intent(body):
    """Detect customer intent"""
    if not body:
        return 'General'
    
    body_lower = str(body).lower()
    
    intents = {
        'Buy': ['اشتري', 'طلب', 'عايز', 'order', 'buy', 'purchase'],
        'Ask Price': ['سعر', 'كام', 'بكام', 'price', 'cost'],
        'Check Availability': ['متوفر', 'موجود', 'available', 'عندكم'],
        'Get Alternative': ['بديل', 'alternative', 'غيره'],
        'Track Order': ['فين', 'وصل', 'الاوردر', 'tracking', 'status'],
        'Get Medical Advice': ['ينفع', 'اخد', 'جرعة', 'علاج', 'دواء'],
        'Report Problem': ['مشكلة', 'شكوى', 'problem', 'issue'],
        'Give Feedback': ['شكرا', 'تمام', 'ممتاز', 'thanks', 'feedback']
    }
    
    for intent, keywords in intents.items():
        for keyword in keywords:
            if keyword in body_lower:
                return intent
    
    return 'General'

def extract_customer_phone(msg):
    """Extract customer phone number"""
    
    # Try multiple possible field locations
    phone = None
    
    # Direct fields
    possible_fields = [
        'remoteJid',
        'from', 
        'to',
        'sender',
        'chatId',
        'participant',
        'jid',
        'phone',
        'number',
        'contact'
    ]
    
    for field in possible_fields:
        if field in msg and msg[field]:
            phone = msg[field]
            break
    
    # Nested in 'key' object
    if not phone and 'key' in msg and isinstance(msg['key'], dict):
        key = msg['key']
        for field in ['remoteJid', 'participant', 'from', 'to']:
            if field in key and key[field]:
                phone = key[field]
                break
    
    # Nested in other objects
    if not phone:
        for parent in ['message', 'chat', 'contact', 'sender']:
            if parent in msg and isinstance(msg[parent], dict):
                for field in ['jid', 'phone', 'id', 'number']:
                    if field in msg[parent]:
                        phone = msg[parent][field]
                        break
    
    # Clean the phone number
    if phone and isinstance(phone, str):
        # Remove @s.whatsapp.net, @c.us, etc.
        phone = re.sub(r'@.*', '', phone)
        # Keep only digits and +
        phone = re.sub(r'[^\d+]', '', phone)
        
        if len(phone) >= 10:
            return phone
    
    return None

def load_json_file(filepath):
    """Load small JSON file"""
    try:
        print(f"   Loading: {os.path.basename(filepath)}")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"   ✅ Loaded {len(data) if isinstance(data, list) else 1} items")
        return data
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []

def create_database(db_path):
    """Create SQLite database with all tables"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Messages table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT,
            direction TEXT,
            type TEXT,
            status TEXT,
            body TEXT,
            body_length INTEGER,
            word_count INTEGER,
            is_broadcast INTEGER,
            is_deleted INTEGER,
            is_group INTEGER,
            has_question INTEGER,
            has_emoji INTEGER,
            has_link INTEGER,
            customer_phone TEXT,
            instance_id TEXT,
            instance_name TEXT,
            company_id TEXT,
            company_name TEXT,
            category TEXT,
            sentiment TEXT,
            urgency TEXT,
            intent TEXT,
            timestamp TEXT,
            date TEXT,
            hour INTEGER,
            day_of_week TEXT,
            month TEXT,
            week TEXT
        )
    ''')
    
    # Companies table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id TEXT PRIMARY KEY,
            name TEXT,
            data TEXT
        )
    ''')
    
    # Instances table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS instances (
            id TEXT PRIMARY KEY,
            name TEXT,
            company_id TEXT,
            phone TEXT,
            data TEXT
        )
    ''')
    
    # Broadcasts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS broadcasts (
            id TEXT PRIMARY KEY,
            name TEXT,
            data TEXT
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_direction ON messages(direction)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_category ON messages(category)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_sentiment ON messages(sentiment)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_customer ON messages(customer_phone)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_instance ON messages(instance_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_company ON messages(company_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_messages_hour ON messages(hour)')
    
    conn.commit()
    
    return conn

def stream_load_messages(json_path, conn, instances_dict, companies_dict, batch_size=5000):
    """Stream load large JSON file into SQLite"""
    
    cursor = conn.cursor()
    
    file_size = os.path.getsize(json_path)
    print(f"\n📂 Loading: {os.path.basename(json_path)}")
    print(f"📊 File size: {file_size / (1024**3):.2f} GB")
    print(f"⏳ This may take 10-30 minutes for large files...")
    
    total_messages = 0
    batch = []
    
    start_time = datetime.now()
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            # Find start of array
            char = f.read(1)
            while char and char != '[':
                char = f.read(1)
            
            if not char:
                print("❌ Could not find JSON array")
                return 0
            
            # Parse messages one by one
            buffer = ""
            brace_depth = 0
            in_string = False
            escape_next = False
            
            while True:
                char = f.read(1)
                
                if not char:
                    break
                
                if char == ']' and brace_depth == 0:
                    break
                
                if char == '{' and brace_depth == 0:
                    buffer = char
                    brace_depth = 1
                    in_string = False
                    escape_next = False
                elif brace_depth > 0:
                    buffer += char
                    
                    if escape_next:
                        escape_next = False
                        continue
                    
                    if char == '\\' and in_string:
                        escape_next = True
                        continue
                    
                    if char == '"':
                        in_string = not in_string
                    elif not in_string:
                        if char == '{':
                            brace_depth += 1
                        elif char == '}':
                            brace_depth -= 1
                            
                            if brace_depth == 0:
                                # Parse complete message
                                try:
                                    msg = json.loads(buffer)
                                    
                                    # Get related data
                                    instance_id = extract_foreign_key(msg, ['instance', 'instanceId', 'instance_id'])
                                    instance = instances_dict.get(instance_id) if instance_id else None
                                    
                                    company = None
                                    company_id = None
                                    if instance:
                                        company_id = extract_foreign_key(instance, ['company', 'companyId', 'company_id'])
                                        company = companies_dict.get(company_id)
                                    
                                    # Parse timestamp
                                    timestamp = parse_timestamp(msg.get('createdAt') or msg.get('timestamp'))
                                    
                                    # Get body
                                    body = msg.get('body', '') or ''
                                    
                                    # Check broadcast
                                    broadcast_id = extract_foreign_key(msg, ['broadCastId', 'broadcast_id', 'broadcast'])
                                    
                                    # Create row
                                    row = (
                                        extract_id(msg),  # message_id
                                        'Outgoing' if msg.get('fromMe', False) else 'Incoming',  # direction
                                        msg.get('type', 'unknown'),  # type
                                        msg.get('status', 'unknown'),  # status
                                        body,  # body
                                        len(str(body)) if body else 0,  # body_length
                                        len(str(body).split()) if body else 0,  # word_count
                                        1 if (msg.get('isBroadCast', False) or broadcast_id) else 0,  # is_broadcast
                                        1 if msg.get('isDeleted', False) else 0,  # is_deleted
                                        1 if msg.get('isGroup', False) else 0,  # is_group
                                        1 if ('?' in str(body) or '؟' in str(body)) else 0,  # has_question
                                        1 if re.search(r'[\U0001F600-\U0001F64F]', str(body)) else 0,  # has_emoji
                                        1 if re.search(r'http[s]?://', str(body)) else 0,  # has_link
                                        extract_customer_phone(msg),  # customer_phone
                                        instance_id,  # instance_id
                                        instance.get('name', 'Unknown') if instance else 'Unknown',  # instance_name
                                        company_id,  # company_id
                                        company.get('name', 'Unknown') if company else 'Unknown',  # company_name
                                        categorize_message(body),  # category
                                        detect_sentiment(body),  # sentiment
                                        detect_urgency(body),  # urgency
                                        detect_intent(body),  # intent
                                        timestamp.isoformat() if timestamp else None,  # timestamp
                                        timestamp.strftime('%Y-%m-%d') if timestamp else None,  # date
                                        timestamp.hour if timestamp else None,  # hour
                                        timestamp.strftime('%A') if timestamp else None,  # day_of_week
                                        timestamp.strftime('%Y-%m') if timestamp else None,  # month
                                        timestamp.strftime('%Y-W%W') if timestamp else None  # week
                                    )
                                    
                                    batch.append(row)
                                    total_messages += 1
                                    
                                    # Insert batch
                                    if len(batch) >= batch_size:
                                        cursor.executemany('''
                                            INSERT INTO messages (
                                                message_id, direction, type, status, body, body_length, word_count,
                                                is_broadcast, is_deleted, is_group, has_question, has_emoji, has_link,
                                                customer_phone, instance_id, instance_name, company_id, company_name,
                                                category, sentiment, urgency, intent, timestamp, date, hour,
                                                day_of_week, month, week
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        ''', batch)
                                        conn.commit()
                                        
                                        elapsed = (datetime.now() - start_time).total_seconds()
                                        rate = total_messages / elapsed if elapsed > 0 else 0
                                        
                                        print(f"   📊 {total_messages:,} messages | {rate:.0f} msg/sec | {elapsed/60:.1f} min")
                                        
                                        batch = []
                                        gc.collect()
                                
                                except json.JSONDecodeError:
                                    pass
                                except Exception as e:
                                    pass
                                
                                buffer = ""
            
            # Insert remaining
            if batch:
                cursor.executemany('''
                    INSERT INTO messages (
                        message_id, direction, type, status, body, body_length, word_count,
                        is_broadcast, is_deleted, is_group, has_question, has_emoji, has_link,
                        customer_phone, instance_id, instance_name, company_id, company_name,
                        category, sentiment, urgency, intent, timestamp, date, hour,
                        day_of_week, month, week
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                conn.commit()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print(f"\n✅ Loading complete!")
        print(f"   📊 Total messages: {total_messages:,}")
        print(f"   ⏱️ Time: {elapsed/60:.1f} minutes")
        print(f"   ⚡ Speed: {total_messages/elapsed:.0f} messages/second")
        
        return total_messages
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    print("="*70)
    print("   FULL DATASET LOADER")
    print("   Load entire JSON into SQLite for analysis")
    print("="*70)
    
    # Get folder path
    folder_path = input("\nEnter folder path: ").strip().strip('"')
    
    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return
    
    # Find files
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    
    print(f"\n📁 Found {len(json_files)} JSON files:")
    
    for i, f in enumerate(json_files, 1):
        size = os.path.getsize(os.path.join(folder_path, f))
        if size > 1024**3:
            size_str = f"{size/(1024**3):.2f} GB"
        else:
            size_str = f"{size/(1024**2):.2f} MB"
        print(f"   [{i}] {f} ({size_str})")
    
    # Select messages file
    print("\n" + "-"*50)
    msg_choice = input("Select MESSAGES file number (the large one): ").strip()
    
    try:
        messages_file = json_files[int(msg_choice) - 1]
    except:
        print("❌ Invalid selection!")
        return
    
    # Find other files automatically
    companies_file = None
    instances_file = None
    broadcasts_file = None
    
    for f in json_files:
        if f == messages_file:
            continue
        f_lower = f.lower()
        if 'compan' in f_lower:
            companies_file = f
        elif 'instance' in f_lower:
            instances_file = f
        elif 'broadcast' in f_lower:
            broadcasts_file = f
    
    print(f"\n📋 Files detected:")
    print(f"   Messages: {messages_file}")
    print(f"   Companies: {companies_file or 'Not found'}")
    print(f"   Instances: {instances_file or 'Not found'}")
    print(f"   Broadcasts: {broadcasts_file or 'Not found'}")
    
    proceed = input("\n🔄 Start loading? This may take 10-30 minutes. (y/n): ").strip().lower()
    if proceed != 'y':
        print("Cancelled.")
        return
    
    # Create database
    db_path = os.path.join(folder_path, 'adam_pharmacy_full.db')
    print(f"\n📦 Creating database: {db_path}")
    
    conn = create_database(db_path)
    cursor = conn.cursor()
    
    # Load reference data
    print("\n📥 Loading reference data...")
    
    companies = []
    instances = []
    broadcasts = []
    
    if companies_file:
        companies = load_json_file(os.path.join(folder_path, companies_file))
        for c in companies:
            cid = extract_id(c)
            if cid:
                cursor.execute('INSERT OR REPLACE INTO companies (id, name, data) VALUES (?, ?, ?)',
                             (cid, c.get('name', ''), json.dumps(c)))
        conn.commit()
    
    if instances_file:
        instances = load_json_file(os.path.join(folder_path, instances_file))
        for i in instances:
            iid = extract_id(i)
            if iid:
                cursor.execute('INSERT OR REPLACE INTO instances (id, name, company_id, phone, data) VALUES (?, ?, ?, ?, ?)',
                             (iid, i.get('name', ''), extract_foreign_key(i, ['company', 'companyId']), 
                              i.get('phone', ''), json.dumps(i)))
        conn.commit()
    
    if broadcasts_file:
        broadcasts = load_json_file(os.path.join(folder_path, broadcasts_file))
        for b in broadcasts:
            bid = extract_id(b)
            if bid:
                cursor.execute('INSERT OR REPLACE INTO broadcasts (id, name, data) VALUES (?, ?, ?)',
                             (bid, b.get('name', ''), json.dumps(b)))
        conn.commit()
    
    # Build lookup dictionaries
    companies_dict = {extract_id(c): c for c in companies if extract_id(c)}
    instances_dict = {extract_id(i): i for i in instances if extract_id(i)}
    
    print(f"   Companies: {len(companies_dict)}")
    print(f"   Instances: {len(instances_dict)}")
    print(f"   Broadcasts: {len(broadcasts)}")
    
    # Load messages
    messages_path = os.path.join(folder_path, messages_file)
    total = stream_load_messages(messages_path, conn, instances_dict, companies_dict)
    
    # Final stats
    print("\n" + "="*70)
    print("✅ DATABASE CREATED SUCCESSFULLY!")
    print("="*70)
    
    cursor.execute('SELECT COUNT(*) FROM messages')
    msg_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT customer_phone) FROM messages WHERE customer_phone IS NOT NULL')
    customer_count = cursor.fetchone()[0]
    
    db_size = os.path.getsize(db_path) / (1024**3)
    
    print(f"   📊 Total messages: {msg_count:,}")
    print(f"   👥 Unique customers: {customer_count:,}")
    print(f"   💾 Database size: {db_size:.2f} GB")
    print(f"   📄 Database file: {db_path}")
    print("="*70)
    
    print("\n🎉 Now run the dashboard with:")
    print(f"   streamlit run dashboard_sqlite.py")
    
    conn.close()

if __name__ == "__main__":
    main()