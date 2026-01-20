import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import re
from collections import Counter
from datetime import datetime, timedelta
import json

# Word cloud
try:
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    WORDCLOUD_AVAILABLE = True
except:
    WORDCLOUD_AVAILABLE = False

# Page config
st.set_page_config(
    page_title="Adam Pharmacies - Super Analytics",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700&display=swap');
    
    * { font-family: 'Cairo', sans-serif; }
    
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #e94560, #0f3460, #00d9ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 20px 0;
    }
    
    .kpi-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 15px;
        padding: 20px;
        text-align: center;
        border: 1px solid #0f3460;
        margin: 5px;
    }
    
    .kpi-value {
        font-size: 2.5rem;
        font-weight: bold;
        color: #e94560;
    }
    
    .kpi-label {
        color: #888;
        font-size: 0.9rem;
    }
    
    .alert-box {
        background: linear-gradient(135deg, #ff6b6b20, #ff6b6b10);
        border-left: 5px solid #ff6b6b;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    
    .success-box {
        background: linear-gradient(135deg, #6bcb7720, #6bcb7710);
        border-left: 5px solid #6bcb77;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    
    .info-box {
        background: linear-gradient(135deg, #00d9ff20, #00d9ff10);
        border-left: 5px solid #00d9ff;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    
    .warning-box {
        background: linear-gradient(135deg, #ffd93d20, #ffd93d10);
        border-left: 5px solid #ffd93d;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
    }
    
    .product-tag {
        display: inline-block;
        background: linear-gradient(90deg, #e94560, #ff6b6b);
        color: white;
        padding: 5px 15px;
        border-radius: 20px;
        margin: 3px;
        font-size: 0.9rem;
    }
    
    .urgent-badge {
        background: #ff6b6b;
        color: white;
        padding: 3px 10px;
        border-radius: 10px;
        font-size: 0.8rem;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    .conversation-out {
        background: #0f3460;
        border-radius: 15px 15px 5px 15px;
        padding: 15px;
        margin: 10px 0 10px 50px;
        border: 1px solid #00d9ff;
    }
    
    .conversation-in {
        background: #1a1a2e;
        border-radius: 15px 15px 15px 5px;
        padding: 15px;
        margin: 10px 50px 10px 0;
        border: 1px solid #e94560;
    }
    
    .metric-row {
        display: flex;
        justify-content: space-around;
        flex-wrap: wrap;
        margin: 20px 0;
    }
    
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
""", unsafe_allow_html=True)

# ============== DATABASE FUNCTIONS ==============

@st.cache_resource
def get_db_connection(db_path):
    """Get database connection"""
    return sqlite3.connect(db_path, check_same_thread=False)

@st.cache_data(ttl=300)
def get_total_stats(_conn):
    """Get total statistics"""
    cursor = _conn.cursor()
    stats = {}
    
    queries = {
        'total': 'SELECT COUNT(*) FROM messages',
        'outgoing': "SELECT COUNT(*) FROM messages WHERE direction = 'Outgoing'",
        'incoming': "SELECT COUNT(*) FROM messages WHERE direction = 'Incoming'",
        'broadcasts': "SELECT COUNT(*) FROM messages WHERE is_broadcast = 1",
        'customers': "SELECT COUNT(DISTINCT customer_phone) FROM messages WHERE customer_phone IS NOT NULL",
        'instances': "SELECT COUNT(DISTINCT instance_name) FROM messages",
        'companies': "SELECT COUNT(DISTINCT company_name) FROM messages",
        'deleted': "SELECT COUNT(*) FROM messages WHERE is_deleted = 1",
        'groups': "SELECT COUNT(*) FROM messages WHERE is_group = 1",
        'questions': "SELECT COUNT(*) FROM messages WHERE has_question = 1",
        'urgent': "SELECT COUNT(*) FROM messages WHERE urgency = 'Urgent'",
        'negative': "SELECT COUNT(*) FROM messages WHERE sentiment = 'Negative'",
        'positive': "SELECT COUNT(*) FROM messages WHERE sentiment = 'Positive'"
    }
    
    for key, query in queries.items():
        cursor.execute(query)
        stats[key] = cursor.fetchone()[0]
    
    return stats

@st.cache_data(ttl=300)
def get_aggregated_stats(_conn, group_by, filters=None, limit=50):
    """Get aggregated statistics"""
    where_clauses = ["1=1"]
    params = []
    
    if filters:
        if filters.get('direction') and filters['direction'] != 'All':
            where_clauses.append("direction = ?")
            params.append(filters['direction'])
        if filters.get('category') and filters['category'] != 'All':
            where_clauses.append("category = ?")
            params.append(filters['category'])
        if filters.get('sentiment') and filters['sentiment'] != 'All':
            where_clauses.append("sentiment = ?")
            params.append(filters['sentiment'])
        if filters.get('company') and filters['company'] != 'All':
            where_clauses.append("company_name = ?")
            params.append(filters['company'])
        if filters.get('instance') and filters['instance'] != 'All':
            where_clauses.append("instance_name = ?")
            params.append(filters['instance'])
        if filters.get('urgency') and filters['urgency'] != 'All':
            where_clauses.append("urgency = ?")
            params.append(filters['urgency'])
    
    query = f"""
        SELECT {group_by}, COUNT(*) as count
        FROM messages
        WHERE {' AND '.join(where_clauses)}
        GROUP BY {group_by}
        ORDER BY count DESC
        LIMIT {limit}
    """
    
    return pd.read_sql_query(query, _conn, params=params)

@st.cache_data(ttl=300)
def get_time_series(_conn, filters=None):
    """Get time series data"""
    where_clauses = ["date IS NOT NULL"]
    params = []
    
    if filters:
        if filters.get('direction') and filters['direction'] != 'All':
            where_clauses.append("direction = ?")
            params.append(filters['direction'])
    
    query = f"""
        SELECT date, direction, COUNT(*) as count
        FROM messages
        WHERE {' AND '.join(where_clauses)}
        GROUP BY date, direction
        ORDER BY date
    """
    
    return pd.read_sql_query(query, _conn, params=params)

@st.cache_data(ttl=300)
def get_hourly_stats(_conn, filters=None):
    """Get hourly statistics"""
    where_clauses = ["hour IS NOT NULL"]
    params = []
    
    if filters:
        if filters.get('direction') and filters['direction'] != 'All':
            where_clauses.append("direction = ?")
            params.append(filters['direction'])
    
    query = f"""
        SELECT hour, direction, COUNT(*) as count
        FROM messages
        WHERE {' AND '.join(where_clauses)}
        GROUP BY hour, direction
        ORDER BY hour
    """
    
    return pd.read_sql_query(query, _conn, params=params)

@st.cache_data(ttl=300)
def get_daily_stats(_conn):
    """Get day of week statistics"""
    query = """
        SELECT day_of_week, COUNT(*) as count
        FROM messages
        WHERE day_of_week IS NOT NULL
        GROUP BY day_of_week
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_heatmap_data(_conn):
    """Get heatmap data (day x hour)"""
    query = """
        SELECT day_of_week, hour, COUNT(*) as count
        FROM messages
        WHERE day_of_week IS NOT NULL AND hour IS NOT NULL
        GROUP BY day_of_week, hour
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=600)
def get_top_words(_conn, direction=None, limit=100):
    """Get top words from messages"""
    
    where_clause = "body IS NOT NULL AND body != ''"
    params = []
    
    if direction and direction != 'All':
        where_clause += " AND direction = ?"
        params.append(direction)
    
    query = f"SELECT body FROM messages WHERE {where_clause} LIMIT 200000"
    df = pd.read_sql_query(query, _conn, params=params)
    
    stopwords = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'have',
        'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
        'this', 'that', 'these', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
        'what', 'which', 'who', 'when', 'where', 'why', 'how', 'all', 'each',
        'every', 'both', 'few', 'more', 'most', 'other', 'some', 'such', 'no',
        'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
        'just', 'hi', 'hello', 'ok', 'okay', 'yes', 'please', 'thanks', 'thank',
        'في', 'من', 'على', 'إلى', 'عن', 'مع', 'هذا', 'هذه', 'ذلك', 'تلك',
        'التي', 'الذي', 'هو', 'هي', 'أنا', 'نحن', 'أنت', 'هم', 'كان', 'كانت',
        'يكون', 'أن', 'إن', 'لا', 'ما', 'لم', 'لن', 'قد', 'و', 'أو', 'ثم',
        'بل', 'لكن', 'حتى', 'إذا', 'لو', 'كل', 'بعض', 'غير', 'بين', 'بعد',
        'قبل', 'دي', 'ده', 'دا', 'اللي', 'بس', 'كده', 'ازاي', 'ليه', 'فين',
        'امتى', 'عشان', 'علشان', 'يعني', 'طيب', 'اه', 'لأ', 'اوك', 'تمام',
        'ماشي', 'السلام', 'عليكم', 'صباح', 'الخير', 'مساء', 'شكرا', 'الله'
    }
    
    all_words = []
    for body in df['body'].dropna():
        text = re.sub(r'http\S+|www\S+', '', str(body))
        text = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', text)
        words = text.lower().split()
        all_words.extend([w for w in words if len(w) >= 2 and w not in stopwords])
    
    return Counter(all_words).most_common(limit)

@st.cache_data(ttl=600)
def get_phrases(_conn, n=2, limit=50):
    """Get common phrases"""
    query = "SELECT body FROM messages WHERE body IS NOT NULL AND body != '' LIMIT 100000"
    df = pd.read_sql_query(query, _conn)
    
    stopwords = {
        'في', 'من', 'على', 'إلى', 'عن', 'مع', 'و', 'أو', 'the', 'a', 'an', 'and', 'or',
        'دي', 'ده', 'دا', 'اللي', 'بس', 'يعني', 'is', 'are', 'was', 'were'
    }
    
    all_phrases = []
    for body in df['body'].dropna():
        text = re.sub(r'http\S+|www\S+', '', str(body))
        text = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', text)
        words = [w for w in text.lower().split() if len(w) >= 2 and w not in stopwords]
        
        for i in range(len(words) - n + 1):
            phrase = ' '.join(words[i:i+n])
            all_phrases.append(phrase)
    
    return Counter(all_phrases).most_common(limit)

@st.cache_data(ttl=300)
def get_customer_stats(_conn, limit=100):
    """Get customer statistics"""
    query = f"""
        SELECT 
            customer_phone,
            COUNT(*) as total_messages,
            SUM(CASE WHEN direction = 'Incoming' THEN 1 ELSE 0 END) as incoming,
            SUM(CASE WHEN direction = 'Outgoing' THEN 1 ELSE 0 END) as outgoing,
            SUM(CASE WHEN is_broadcast = 1 THEN 1 ELSE 0 END) as broadcasts,
            SUM(CASE WHEN sentiment = 'Positive' THEN 1 ELSE 0 END) as positive,
            SUM(CASE WHEN sentiment = 'Negative' THEN 1 ELSE 0 END) as negative,
            MIN(date) as first_contact,
            MAX(date) as last_contact
        FROM messages
        WHERE customer_phone IS NOT NULL
        GROUP BY customer_phone
        ORDER BY total_messages DESC
        LIMIT {limit}
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_customer_segments(_conn):
    """Get customer segmentation"""
    query = """
        SELECT 
            customer_phone,
            COUNT(*) as msg_count
        FROM messages
        WHERE customer_phone IS NOT NULL
        GROUP BY customer_phone
    """
    df = pd.read_sql_query(query, _conn)
    
    segments = {
        'New (1 msg)': len(df[df['msg_count'] == 1]),
        'Active (2-5)': len(df[(df['msg_count'] >= 2) & (df['msg_count'] <= 5)]),
        'Engaged (6-10)': len(df[(df['msg_count'] >= 6) & (df['msg_count'] <= 10)]),
        'Loyal (11-50)': len(df[(df['msg_count'] >= 11) & (df['msg_count'] <= 50)]),
        'VIP (50+)': len(df[df['msg_count'] > 50])
    }
    
    return pd.DataFrame(list(segments.items()), columns=['Segment', 'Count'])

@st.cache_data(ttl=300)
def get_urgent_messages(_conn, limit=50):
    """Get urgent messages"""
    query = f"""
        SELECT direction, category, sentiment, instance_name, company_name, 
               customer_phone, body, date, urgency
        FROM messages
        WHERE urgency = 'Urgent'
        ORDER BY date DESC
        LIMIT {limit}
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_negative_messages(_conn, limit=50):
    """Get negative messages"""
    query = f"""
        SELECT direction, category, instance_name, company_name, 
               customer_phone, body, date
        FROM messages
        WHERE sentiment = 'Negative'
        ORDER BY date DESC
        LIMIT {limit}
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_questions(_conn, limit=50):
    """Get unanswered questions"""
    query = f"""
        SELECT direction, category, instance_name, customer_phone, body, date
        FROM messages
        WHERE has_question = 1 AND direction = 'Incoming'
        ORDER BY date DESC
        LIMIT {limit}
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_conversation(_conn, customer_phone, limit=50):
    """Get conversation for a customer"""
    query = f"""
        SELECT direction, body, date, timestamp, category, sentiment
        FROM messages
        WHERE customer_phone = ?
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    return pd.read_sql_query(query, _conn, params=[customer_phone])

@st.cache_data(ttl=300)
def get_instance_stats(_conn):
    """Get detailed instance statistics"""
    query = """
        SELECT 
            instance_name,
            company_name,
            COUNT(*) as total,
            SUM(CASE WHEN direction = 'Outgoing' THEN 1 ELSE 0 END) as sent,
            SUM(CASE WHEN direction = 'Incoming' THEN 1 ELSE 0 END) as received,
            SUM(CASE WHEN is_broadcast = 1 THEN 1 ELSE 0 END) as broadcasts,
            SUM(CASE WHEN sentiment = 'Positive' THEN 1 ELSE 0 END) as positive,
            SUM(CASE WHEN sentiment = 'Negative' THEN 1 ELSE 0 END) as negative,
            AVG(body_length) as avg_length
        FROM messages
        GROUP BY instance_name, company_name
        ORDER BY total DESC
        LIMIT 50
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_company_stats(_conn):
    """Get company statistics"""
    query = """
        SELECT 
            company_name,
            COUNT(*) as total,
            SUM(CASE WHEN direction = 'Outgoing' THEN 1 ELSE 0 END) as sent,
            SUM(CASE WHEN direction = 'Incoming' THEN 1 ELSE 0 END) as received,
            SUM(CASE WHEN is_broadcast = 1 THEN 1 ELSE 0 END) as broadcasts,
            COUNT(DISTINCT instance_name) as instances,
            COUNT(DISTINCT customer_phone) as customers
        FROM messages
        GROUP BY company_name
        ORDER BY total DESC
    """
    return pd.read_sql_query(query, _conn)

@st.cache_data(ttl=300)
def get_product_mentions(_conn):
    """Get medicine/product mentions"""
    query = "SELECT body FROM messages WHERE body IS NOT NULL LIMIT 100000"
    df = pd.read_sql_query(query, _conn)
    
    medicines = [
        'بنادول', 'بروفين', 'فولتارين', 'اوجمنتين', 'فلاجيل', 'انتينال',
        'ازيثروميسين', 'اموكسيسيللين', 'سيبروفلوكساسين', 'باراسيتامول',
        'ايبوبروفين', 'اسبرين', 'فيتامين', 'كالسيوم', 'حديد', 'زنك',
        'panadol', 'brufen', 'voltaren', 'augmentin', 'flagyl', 'antinal',
        'vitamin', 'calcium', 'iron', 'zinc', 'omega'
    ]
    
    symptoms = [
        'صداع', 'سخونة', 'حرارة', 'كحة', 'سعال', 'برد', 'انفلونزا', 'الم',
        'وجع', 'حساسية', 'ضغط', 'سكر', 'مغص', 'اسهال', 'امساك', 'غثيان',
        'headache', 'fever', 'cough', 'cold', 'flu', 'pain', 'allergy'
    ]
    
    product_counts = Counter()
    symptom_counts = Counter()
    
    for body in df['body'].dropna():
        body_lower = str(body).lower()
        for med in medicines:
            if med.lower() in body_lower:
                product_counts[med] += 1
        for sym in symptoms:
            if sym.lower() in body_lower:
                symptom_counts[sym] += 1
    
    return product_counts.most_common(20), symptom_counts.most_common(20)

@st.cache_data(ttl=300)
def search_messages(_conn, query_text, direction=None, category=None, limit=100):
    """Search messages"""
    where_clauses = ["body LIKE ?"]
    params = [f'%{query_text}%']
    
    if direction and direction != 'All':
        where_clauses.append("direction = ?")
        params.append(direction)
    
    if category and category != 'All':
        where_clauses.append("category = ?")
        params.append(category)
    
    query = f"""
        SELECT direction, category, sentiment, urgency, instance_name, 
               company_name, customer_phone, body, date
        FROM messages
        WHERE {' AND '.join(where_clauses)}
        ORDER BY date DESC
        LIMIT {limit}
    """
    
    return pd.read_sql_query(query, _conn, params=params)

def generate_wordcloud_fig(word_freq):
    """Generate word cloud figure"""
    if not WORDCLOUD_AVAILABLE or not word_freq:
        return None
    
    try:
        wc = WordCloud(
            width=800, height=400,
            background_color='#1a1a2e',
            colormap='plasma',
            max_words=80
        ).generate_from_frequencies(dict(word_freq))
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wc, interpolation='bilinear')
        ax.axis('off')
        fig.patch.set_facecolor('#1a1a2e')
        return fig
    except:
        return None

def create_gauge(value, title, max_val=100):
    """Create gauge chart"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title, 'font': {'size': 12, 'color': 'white'}},
        gauge={
            'axis': {'range': [0, max_val], 'tickcolor': 'white'},
            'bar': {'color': "#e94560"},
            'bgcolor': "#1a1a2e",
            'steps': [
                {'range': [0, max_val*0.3], 'color': '#ff6b6b'},
                {'range': [max_val*0.3, max_val*0.7], 'color': '#ffd93d'},
                {'range': [max_val*0.7, max_val], 'color': '#6bcb77'}
            ]
        },
        number={'font': {'color': 'white', 'size': 18}}
    ))
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        height=150,
        margin=dict(l=10, r=10, t=40, b=10)
    )
    return fig

def generate_recommendations(stats):
    """Generate AI recommendations based on stats"""
    recommendations = []
    
    total = stats['total']
    if total == 0:
        return recommendations
    
    # Response rate
    out_ratio = stats['outgoing'] / total * 100
    if out_ratio < 40:
        recommendations.append({
            'icon': '📤',
            'title': 'Low Outgoing Rate',
            'text': f'Only {out_ratio:.1f}% of messages are outgoing. Consider more proactive communication.',
            'priority': 'warning',
            'action': 'Increase proactive messaging and follow-ups'
        })
    
    # Negative sentiment
    neg_ratio = stats['negative'] / total * 100
    if neg_ratio > 10:
        recommendations.append({
            'icon': '😞',
            'title': 'High Negative Sentiment',
            'text': f'{neg_ratio:.1f}% of messages are negative. Review customer complaints.',
            'priority': 'high',
            'action': 'Investigate top complaints and create solutions'
        })
    
    # Urgent messages
    if stats['urgent'] > 0:
        recommendations.append({
            'icon': '🚨',
            'title': 'Urgent Messages',
            'text': f'{stats["urgent"]:,} urgent messages need immediate attention.',
            'priority': 'high',
            'action': 'Review urgent messages in Alerts tab'
        })
    
    # Broadcast usage
    broadcast_ratio = stats['broadcasts'] / total * 100
    if broadcast_ratio < 5:
        recommendations.append({
            'icon': '📢',
            'title': 'Low Broadcast Usage',
            'text': f'Only {broadcast_ratio:.1f}% are broadcasts. More marketing opportunity.',
            'priority': 'medium',
            'action': 'Create promotional broadcast campaigns'
        })
    elif broadcast_ratio > 30:
        recommendations.append({
            'icon': '📢',
            'title': 'High Broadcast Rate',
            'text': f'{broadcast_ratio:.1f}% are broadcasts. Good marketing presence!',
            'priority': 'success',
            'action': 'Maintain broadcast strategy'
        })
    
    # Questions
    if stats['questions'] > 100:
        recommendations.append({
            'icon': '❓',
            'title': 'Many Customer Questions',
            'text': f'{stats["questions"]:,} questions received. Create FAQ or auto-replies.',
            'priority': 'medium',
            'action': 'Build FAQ based on common questions'
        })
    
    # Positive sentiment
    pos_ratio = stats['positive'] / total * 100
    if pos_ratio > 40:
        recommendations.append({
            'icon': '😊',
            'title': 'Great Customer Satisfaction',
            'text': f'{pos_ratio:.1f}% positive sentiment. Excellent job!',
            'priority': 'success',
            'action': 'Keep up the great work!'
        })
    
    return recommendations

def generate_auto_replies(top_words, categories_df):
    """Generate suggested auto-replies"""
    suggestions = [
        {
            'category': 'Price Inquiry',
            'triggers': ['سعر', 'كام', 'بكام', 'price'],
            'reply': 'مرحباً! يمكنك معرفة أسعار منتجاتنا من خلال موقعنا أو أرسل اسم المنتج وسنرد عليك بالسعر. 💊'
        },
        {
            'category': 'Availability',
            'triggers': ['متوفر', 'موجود', 'عندكم', 'available'],
            'reply': 'أهلاً! لمعرفة توفر المنتج، أرسل لنا اسم الدواء وسنتحقق لك فوراً. ✅'
        },
        {
            'category': 'Delivery',
            'triggers': ['توصيل', 'فين الاوردر', 'delivery', 'وصل'],
            'reply': 'مرحباً! أرسل لنا رقم الطلب وسنخبرك بحالة التوصيل. 🚚'
        },
        {
            'category': 'Greeting',
            'triggers': ['السلام', 'صباح', 'مساء', 'اهلا', 'hello'],
            'reply': 'أهلاً وسهلاً! كيف يمكننا مساعدتك اليوم؟ 😊'
        },
        {
            'category': 'Thanks',
            'triggers': ['شكرا', 'thanks', 'متشكر'],
            'reply': 'العفو! سعداء بخدمتك. لا تتردد في التواصل معنا في أي وقت. ❤️'
        },
        {
            'category': 'Order',
            'triggers': ['طلب', 'اطلب', 'عايز', 'order'],
            'reply': 'أهلاً! لإتمام طلبك، أرسل لنا قائمة المنتجات وعنوان التوصيل. 📦'
        }
    ]
    
    return suggestions

# ============== MAIN APP ==============

def main():
    # Header
    st.markdown('<h1 class="main-header">💊 Adam Pharmacies Super Analytics</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #888;">Complete Customer & Business Intelligence</p>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/3d-fluency/94/pharmacy.png", width=80)
        st.title("📁 Database")
        
        folder_path = st.text_input("Folder:", value="/home/eslamkhalil/Desktop/messagesJSONS")
        db_path = os.path.join(folder_path, "adam_pharmacy_full.db")
        
        if not os.path.exists(db_path):
            st.error("❌ Database not found!")
            st.info("Run `python load_to_database.py` first")
            st.stop()
        
        conn = get_db_connection(db_path)
        
        db_size = os.path.getsize(db_path) / (1024**3)
        st.success(f"✅ Connected ({db_size:.2f} GB)")
        
        # Stats
        stats = get_total_stats(conn)
        
        st.markdown("---")
        st.markdown(f"**📊 {stats['total']:,}** messages")
        st.markdown(f"**👥 {stats['customers']:,}** customers")
        st.markdown(f"**📱 {stats['instances']:,}** instances")
        
        # Filters
        st.markdown("---")
        st.subheader("🔍 Filters")
        
        direction = st.selectbox("Direction:", ['All', 'Incoming', 'Outgoing'])
        
        categories = get_aggregated_stats(conn, 'category')['category'].tolist()
        category = st.selectbox("Category:", ['All'] + categories)
        
        sentiment = st.selectbox("Sentiment:", ['All', 'Positive', 'Neutral', 'Negative'])
        
        urgency = st.selectbox("Urgency:", ['All', 'Normal', 'High', 'Urgent'])
        
        companies = get_aggregated_stats(conn, 'company_name')['company_name'].tolist()
        company = st.selectbox("Company:", ['All'] + companies[:20])
        
        filters = {
            'direction': direction,
            'category': category,
            'sentiment': sentiment,
            'urgency': urgency,
            'company': company
        }
    
    # Urgent Alert
    if stats['urgent'] > 0:
        st.markdown(f"""
        <div class="alert-box">
            <h3>🚨 {stats['urgent']:,} Urgent Messages Need Attention!</h3>
            <p>Check the Alerts tab to review urgent messages.</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Top KPIs
    st.markdown("---")
    
    col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
    
    with col1:
        st.metric("📬 Total", f"{stats['total']:,}")
    with col2:
        st.metric("📤 Out", f"{stats['outgoing']:,}")
    with col3:
        st.metric("📥 In", f"{stats['incoming']:,}")
    with col4:
        st.metric("📢 Broadcast", f"{stats['broadcasts']:,}")
    with col5:
        st.metric("👥 Customers", f"{stats['customers']:,}")
    with col6:
        st.metric("😊 Positive", f"{stats['positive']:,}")
    with col7:
        st.metric("😞 Negative", f"{stats['negative']:,}")
    with col8:
        st.metric("🚨 Urgent", f"{stats['urgent']:,}")
    
    # Tabs
    tabs = st.tabs([
        "📊 Overview",
        "💊 Products",
        "📝 Words",
        "👥 Customers",
        "📂 Categories",
        "😊 Sentiment",
        "⏰ Time",
        "📱 Instances",
        "🏢 Companies",
        "🔔 Alerts",
        "💬 Conversations",
        "🤖 AI Insights",
        "🔍 Search"
    ])
    
    # ============== TAB 0: OVERVIEW ==============
    with tabs[0]:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            dir_df = get_aggregated_stats(conn, 'direction', filters)
            fig = px.pie(dir_df, values='count', names='direction',
                        title="📨 Direction", hole=0.4,
                        color_discrete_sequence=['#00d9ff', '#e94560'])
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            cat_df = get_aggregated_stats(conn, 'category', filters, limit=8)
            fig = px.pie(cat_df, values='count', names='category',
                        title="📂 Categories",
                        color_discrete_sequence=px.colors.qualitative.Set3)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col3:
            sent_df = get_aggregated_stats(conn, 'sentiment', filters)
            colors = {'Positive': '#6bcb77', 'Neutral': '#ffd93d', 'Negative': '#ff6b6b'}
            fig = px.pie(sent_df, values='count', names='sentiment',
                        title="😊 Sentiment", color='sentiment',
                        color_discrete_map=colors)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Gauges
        st.subheader("📈 Performance Metrics")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        total = stats['total']
        pos_rate = stats['positive'] / total * 100 if total > 0 else 0
        out_rate = stats['outgoing'] / total * 100 if total > 0 else 0
        bc_rate = stats['broadcasts'] / total * 100 if total > 0 else 0
        neg_rate = stats['negative'] / total * 100 if total > 0 else 0
        
        with col1:
            st.plotly_chart(create_gauge(pos_rate, "Positive %"), use_container_width=True)
        with col2:
            st.plotly_chart(create_gauge(out_rate, "Outgoing %"), use_container_width=True)
        with col3:
            st.plotly_chart(create_gauge(bc_rate, "Broadcast %"), use_container_width=True)
        with col4:
            st.plotly_chart(create_gauge(100 - neg_rate, "Satisfaction %"), use_container_width=True)
        with col5:
            cust_rate = min(stats['customers'] / 1000, 100)
            st.plotly_chart(create_gauge(cust_rate, "Customers (K)"), use_container_width=True)
        
        # Type distribution
        st.subheader("📝 Message Types")
        type_df = get_aggregated_stats(conn, 'type', filters, limit=10)
        fig = px.bar(type_df, x='count', y='type', orientation='h',
                    color='count', color_continuous_scale='Viridis')
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                        showlegend=False, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
    
    # ============== TAB 1: PRODUCTS ==============
    with tabs[1]:
        st.subheader("💊 Product & Medicine Analysis")
        
        with st.spinner("Analyzing product mentions..."):
            products, symptoms = get_product_mentions(conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 💊 Top Medicines Mentioned")
            if products:
                prod_df = pd.DataFrame(products, columns=['Product', 'Mentions'])
                fig = px.bar(prod_df, x='Mentions', y='Product', orientation='h',
                           color='Mentions', color_continuous_scale='Plasma')
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                                showlegend=False, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No specific product mentions found")
        
        with col2:
            st.markdown("### 🤒 Common Symptoms")
            if symptoms:
                symp_df = pd.DataFrame(symptoms, columns=['Symptom', 'Mentions'])
                fig = px.bar(symp_df, x='Mentions', y='Symptom', orientation='h',
                           color='Mentions', color_continuous_scale='Reds')
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                                showlegend=False, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No symptom mentions found")
    
    # ============== TAB 2: WORDS ==============
    with tabs[2]:
        st.subheader("📝 Word Analysis")
        
        word_direction = st.radio("Analyze words from:", ['All', 'Incoming', 'Outgoing'], horizontal=True)
        
        with st.spinner("Analyzing words..."):
            word_freq = get_top_words(conn, word_direction, 80)
            phrase_freq = get_phrases(conn, n=2, limit=30)
        
        if word_freq:
            col1, col2 = st.columns(2)
            
            with col1:
                words_df = pd.DataFrame(word_freq[:25], columns=['Word', 'Count'])
                fig = px.bar(words_df, x='Count', y='Word', orientation='h',
                           title="🔤 Top 25 Words", color='Count', color_continuous_scale='Plasma')
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                                showlegend=False, yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                wc_fig = generate_wordcloud_fig(word_freq)
                if wc_fig:
                    st.pyplot(wc_fig)
                else:
                    st.markdown("#### 🏷️ Top Words")
                    tags = " ".join([f'<span class="product-tag">{w} ({c})</span>' for w, c in word_freq[:30]])
                    st.markdown(tags, unsafe_allow_html=True)
            
            # Phrases
            if phrase_freq:
                st.subheader("💬 Common Phrases")
                phrases_df = pd.DataFrame(phrase_freq[:20], columns=['Phrase', 'Count'])
                fig = px.bar(phrases_df, x='Count', y='Phrase', orientation='h',
                           color='Count', color_continuous_scale='Viridis')
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                                showlegend=False, yaxis={'categoryorder': 'total ascending'}, height=500)
                st.plotly_chart(fig, use_container_width=True)
            
            # Word table
            with st.expander("📋 Full Word List"):
                st.dataframe(pd.DataFrame(word_freq, columns=['Word', 'Count']), use_container_width=True)
    
    # ============== TAB 3: CUSTOMERS ==============
    with tabs[3]:
        st.subheader("👥 Customer Intelligence")
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("👥 Total Customers", f"{stats['customers']:,}")
        with col2:
            customer_df = get_customer_stats(conn)
            avg_msgs = customer_df['total_messages'].mean() if not customer_df.empty else 0
            st.metric("📊 Avg Msgs/Customer", f"{avg_msgs:.1f}")
        with col3:
            repeat = len(customer_df[customer_df['total_messages'] > 1])
            st.metric("🔄 Repeat Customers", f"{repeat:,}")
        with col4:
            vip = len(customer_df[customer_df['total_messages'] > 10])
            st.metric("⭐ VIP (10+ msgs)", f"{vip:,}")
        
        # Segmentation
        st.subheader("📊 Customer Segmentation")
        
        segments_df = get_customer_segments(conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(segments_df, values='Count', names='Segment',
                        title="Customer Segments",
                        color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(segments_df, x='Count', y='Segment', orientation='h',
                        color='Count', color_continuous_scale='Turbo')
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'}, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        # Top customers
        st.subheader("🏆 Top Customers")
        
        fig = px.bar(customer_df.head(20), x='total_messages', y='customer_phone',
                    orientation='h', color='total_messages', color_continuous_scale='Turbo')
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                        showlegend=False, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("📋 Customer Details"):
            st.dataframe(customer_df, use_container_width=True)
    
    # ============== TAB 4: CATEGORIES ==============
    with tabs[4]:
        st.subheader("📂 Message Categories")
        
        cat_df = get_aggregated_stats(conn, 'category', filters)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(cat_df, values='count', names='category',
                        title="Category Distribution",
                        color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(cat_df, x='count', y='category', orientation='h',
                        color='count', color_continuous_scale='Viridis')
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                            showlegend=False, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Intent
        st.subheader("🎯 Customer Intent")
        intent_df = get_aggregated_stats(conn, 'intent', filters)
        
        fig = px.treemap(intent_df, path=['intent'], values='count',
                        title="Customer Intents",
                        color='count', color_continuous_scale='Plasma')
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
        st.plotly_chart(fig, use_container_width=True)
    
    # ============== TAB 5: SENTIMENT ==============
    with tabs[5]:
        st.subheader("😊 Sentiment Analysis")
        
        sent_df = get_aggregated_stats(conn, 'sentiment', filters)
        total_sent = sent_df['count'].sum()
        
        pos = sent_df[sent_df['sentiment'] == 'Positive']['count'].sum() if not sent_df.empty else 0
        neu = sent_df[sent_df['sentiment'] == 'Neutral']['count'].sum() if not sent_df.empty else 0
        neg = sent_df[sent_df['sentiment'] == 'Negative']['count'].sum() if not sent_df.empty else 0
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #6bcb77, #4a9c5d); padding: 30px; border-radius: 15px; text-align: center;">
                <h1 style="margin: 0;">😊 {pos:,}</h1>
                <h3>{pos/total_sent*100:.1f}% Positive</h3>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #ffd93d, #d4b52a); padding: 30px; border-radius: 15px; text-align: center; color: black;">
                <h1 style="margin: 0;">😐 {neu:,}</h1>
                <h3>{neu/total_sent*100:.1f}% Neutral</h3>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #ff6b6b, #d45555); padding: 30px; border-radius: 15px; text-align: center;">
                <h1 style="margin: 0;">😞 {neg:,}</h1>
                <h3>{neg/total_sent*100:.1f}% Negative</h3>
            </div>
            """, unsafe_allow_html=True)
        
        # Sentiment by category
        st.subheader("📊 Sentiment by Category")
        
        query = """
            SELECT category, sentiment, COUNT(*) as count
            FROM messages
            GROUP BY category, sentiment
        """
        cat_sent_df = pd.read_sql_query(query, conn)
        
        if not cat_sent_df.empty:
            pivot = cat_sent_df.pivot(index='category', columns='sentiment', values='count').fillna(0)
            fig = px.bar(pivot, barmode='group',
                        color_discrete_map={'Positive': '#6bcb77', 'Neutral': '#ffd93d', 'Negative': '#ff6b6b'})
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Negative messages sample
        st.subheader("🔴 Sample Negative Messages")
        neg_msgs = get_negative_messages(conn, limit=10)
        
        for _, row in neg_msgs.iterrows():
            if row['body']:
                st.markdown(f"""
                <div class="alert-box">
                    <strong>📍 {row['category']}</strong> | {row['instance_name']}<br>
                    {str(row['body'])[:200]}{'...' if len(str(row['body'])) > 200 else ''}
                </div>
                """, unsafe_allow_html=True)
    
    # ============== TAB 6: TIME ==============
    with tabs[6]:
        st.subheader("⏰ Time Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            hourly_df = get_hourly_stats(conn, filters)
            if not hourly_df.empty:
                fig = px.bar(hourly_df, x='hour', y='count', color='direction',
                           title="📊 Messages by Hour",
                           color_discrete_map={'Outgoing': '#00d9ff', 'Incoming': '#e94560'},
                           barmode='group')
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            daily_df = get_daily_stats(conn)
            if not daily_df.empty:
                days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                daily_df['day_of_week'] = pd.Categorical(daily_df['day_of_week'], categories=days_order, ordered=True)
                daily_df = daily_df.sort_values('day_of_week')
                
                fig = px.bar(daily_df, x='day_of_week', y='count',
                           title="📅 Messages by Day",
                           color='count', color_continuous_scale='Viridis')
                fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'}, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        
        # Trend
        time_df = get_time_series(conn, filters)
        if not time_df.empty:
            st.subheader("📈 Daily Trend")
            fig = px.line(time_df, x='date', y='count', color='direction',
                        color_discrete_map={'Outgoing': '#00d9ff', 'Incoming': '#e94560'})
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Heatmap
        st.subheader("🔥 Activity Heatmap")
        heatmap_df = get_heatmap_data(conn)
        
        if not heatmap_df.empty:
            pivot = heatmap_df.pivot(index='day_of_week', columns='hour', values='count').fillna(0)
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            pivot = pivot.reindex(days_order)
            
            fig = px.imshow(pivot, labels=dict(x="Hour", y="Day", color="Messages"),
                          color_continuous_scale='YlOrRd', aspect='auto')
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
    
    # ============== TAB 7: INSTANCES ==============
    with tabs[7]:
        st.subheader("📱 Instance Analysis")
        
        instance_df = get_instance_stats(conn)
        
        # Top instances
        fig = px.bar(instance_df.head(20), x='total', y='instance_name', orientation='h',
                    title="Top 20 Instances", color='total', color_continuous_scale='Turbo')
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                        showlegend=False, yaxis={'categoryorder': 'total ascending'}, height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        # Instance details
        st.subheader("📋 Instance Details")
        st.dataframe(instance_df, use_container_width=True)
    
    # ============== TAB 8: COMPANIES ==============
    with tabs[8]:
        st.subheader("🏢 Company Analysis")
        
        company_df = get_company_stats(conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(company_df, values='total', names='company_name',
                        title="Messages by Company",
                        color_discrete_sequence=px.colors.qualitative.Bold)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(company_df, x='total', y='company_name', orientation='h',
                        color='total', color_continuous_scale='Plasma')
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': 'white'},
                            showlegend=False, yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Company details
        st.subheader("📋 Company Details")
        
        for _, row in company_df.iterrows():
            with st.expander(f"🏢 {row['company_name']} ({row['total']:,} messages)"):
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total", f"{row['total']:,}")
                with col2:
                    st.metric("Sent", f"{row['sent']:,}")
                with col3:
                    st.metric("Received", f"{row['received']:,}")
                with col4:
                    st.metric("Instances", f"{row['instances']:,}")
    
    # ============== TAB 9: ALERTS ==============
    with tabs[9]:
        st.subheader("🔔 Alerts & Urgent Messages")
        
        # Urgent messages
        st.markdown(f"### 🚨 Urgent Messages ({stats['urgent']:,})")
        
        urgent_msgs = get_urgent_messages(conn)
        
        if not urgent_msgs.empty:
            for _, row in urgent_msgs.iterrows():
                st.markdown(f"""
                <div class="alert-box">
                    <span class="urgent-badge">URGENT</span>
                    <strong> {row['category']}</strong> | {row['instance_name']}<br>
                    <p>{str(row['body'])[:250] if row['body'] else 'No content'}</p>
                    <small>📅 {row['date']} | 👤 {row['customer_phone'] or 'Unknown'}</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.success("✅ No urgent messages!")
        
        # Unanswered questions
        st.markdown(f"### ❓ Incoming Questions ({stats['questions']:,})")
        
        questions = get_questions(conn, limit=20)
        
        if not questions.empty:
            for _, row in questions.iterrows():
                st.markdown(f"""
                <div class="info-box">
                    <strong>❓ {row['category']}</strong> | {row['instance_name']}<br>
                    <p>{str(row['body'])[:200] if row['body'] else ''}</p>
                </div>
                """, unsafe_allow_html=True)
    
    # ============== TAB 10: CONVERSATIONS ==============
    with tabs[10]:
        st.subheader("💬 Customer Conversations")
        
        # Get customer list
        customer_df = get_customer_stats(conn, limit=200)
        
        if not customer_df.empty:
            selected_customer = st.selectbox(
                "Select customer:",
                customer_df['customer_phone'].tolist(),
                format_func=lambda x: f"{x} ({customer_df[customer_df['customer_phone']==x]['total_messages'].values[0]} msgs)"
            )
            
            if selected_customer:
                conv_df = get_conversation(conn, selected_customer)
                
                st.markdown(f"### Conversation with {selected_customer}")
                st.markdown(f"*{len(conv_df)} messages*")
                
                for _, row in conv_df.iterrows():
                    bubble_class = "conversation-out" if row['direction'] == 'Outgoing' else "conversation-in"
                    icon = "📤" if row['direction'] == 'Outgoing' else "📥"
                    
                    st.markdown(f"""
                    <div class="{bubble_class}">
                        <small>{icon} {row['direction']} - {row['date']} | {row['sentiment']}</small><br>
                        {str(row['body'])[:300] if row['body'] else '[No content]'}
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.warning("No customer data available")
    
    # ============== TAB 11: AI INSIGHTS ==============
    with tabs[11]:
        st.subheader("🤖 AI-Powered Insights")
        
        recommendations = generate_recommendations(stats)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 💡 Recommendations")
            
            if recommendations:
                for rec in recommendations:
                    box_class = {
                        'high': 'alert-box',
                        'warning': 'warning-box',
                        'medium': 'info-box',
                        'success': 'success-box'
                    }.get(rec['priority'], 'info-box')
                    
                    st.markdown(f"""
                    <div class="{box_class}">
                        <h4>{rec['icon']} {rec['title']}</h4>
                        <p>{rec['text']}</p>
                        <strong>🎯 Action:</strong> {rec['action']}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.success("✅ Everything looks great!")
        
        with col2:
            st.markdown("### 💬 Suggested Auto-Replies")
            
            auto_replies = generate_auto_replies([], None)
            
            for ar in auto_replies:
                with st.expander(f"📝 {ar['category']}"):
                    st.markdown(f"**Triggers:** {', '.join(ar['triggers'])}")
                    st.markdown("**Suggested reply:**")
                    st.code(ar['reply'], language=None)
    
    # ============== TAB 12: SEARCH ==============
    with tabs[12]:
        st.subheader("🔍 Search Messages")
        
        search_query = st.text_input("Enter search term:", placeholder="Type a word or phrase...")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            search_dir = st.selectbox("Direction:", ['All', 'Incoming', 'Outgoing'], key='s_dir')
        with col2:
            search_cat = st.selectbox("Category:", ['All'] + categories, key='s_cat')
        with col3:
            max_results = st.slider("Max results:", 10, 200, 50)
        
        if search_query:
            with st.spinner("Searching..."):
                results = search_messages(conn, search_query, search_dir, search_cat, max_results)
            
            st.markdown(f"### Found {len(results):,} results for '{search_query}'")
            
            for _, row in results.iterrows():
                body = str(row['body']) if row['body'] else ''
                highlighted = re.sub(
                    f'({re.escape(search_query)})',
                    r'<mark style="background: #e94560; color: white; padding: 2px 5px; border-radius: 3px;">\1</mark>',
                    body, flags=re.IGNORECASE
                )
                
                urgency_badge = '<span class="urgent-badge">URGENT</span>' if row['urgency'] == 'Urgent' else ''
                
                st.markdown(f"""
                <div style="background: #0f3460; padding: 15px; border-radius: 10px; margin: 10px 0; border-left: 4px solid {'#00d9ff' if row['direction'] == 'Outgoing' else '#e94560'};">
                    {urgency_badge}
                    <strong>{'📤' if row['direction'] == 'Outgoing' else '📥'} {row['direction']}</strong> | 
                    {row['category']} | {row['sentiment']} | {row['instance_name']}<br>
                    <p>{highlighted[:400]}{'...' if len(body) > 400 else ''}</p>
                    <small>📅 {row['date']} | 👤 {row['customer_phone'] or 'N/A'}</small>
                </div>
                """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 Export Stats to CSV", use_container_width=True):
            stats_df = pd.DataFrame([stats])
            csv = stats_df.to_csv(index=False)
            st.download_button("Download", csv, "stats.csv", "text/csv")
    
    with col2:
        if st.button("📊 Export Customer Data", use_container_width=True):
            cust_df = get_customer_stats(conn, limit=1000)
            csv = cust_df.to_csv(index=False)
            st.download_button("Download", csv, "customers.csv", "text/csv")
    
    with col3:
        if st.button("📋 Export Word Frequency", use_container_width=True):
            words = get_top_words(conn, None, 200)
            words_df = pd.DataFrame(words, columns=['Word', 'Count'])
            csv = words_df.to_csv(index=False)
            st.download_button("Download", csv, "words.csv", "text/csv")
    
    st.markdown(f"""
    <p style="text-align: center; color: #666; margin-top: 20px;">
        📊 Analyzing <strong>{stats['total']:,}</strong> messages from <strong>{stats['customers']:,}</strong> customers
    </p>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()