import pandas as pd
from dateutil import parser
from sklearn.preprocessing import StandardScaler
from datetime import datetime
from masterorder import get_master_order
from utility import days_between, calculate_discount_mean, categorize_review_count

def get_user(input_path):
    return pd.read_csv(input_path + "/user.csv")

def get_review(input_path):
    # merge review data with user data
    review= pd.read_csv(input_path + "/user.csv")
    user= get_user(input_path)
    merged_df = review.merge(user, how='left', left_on='고객 이메일', right_on='email')
    merged_df.rename(columns={'id_y': 'user id', 'id_x': 'id'}, inplace=True)
    merged_df.drop(columns=['email', '고객 이메일', 'brandpay_joined', 'date_joined', 'left_at', 'invitation_code', 'is_sms',
                            'is_news', 'phone_number', 'key', 'name', 'status', 'last_visited_at', 'normal_purchase_amount',
                            'morning_purchase_amount', 'regular_purchase_amount', 'gift_purchase_amount', 'quick_purchase_amount',
                            'total_purchase_amount', 'normal_purchase_count', 'morning_purchase_count', 'quick_purchase_count',
                            'regular_purchase_count', 'claim_count', 'gift_purchase_count', 'total_purchase_count', 'birth',
                            'first_purchased_at', 'second_purchased_at', 'first_purchase_days', 'second_purchase_days',
                            'last_purchased_at', 'pre_dormant_email_sent_at', 'old_id', 'toss_access_token', 'toss_refresh_token',
                            'flower_taste'], inplace=True)
    return merged_df


def standardize_data(df):
    features_to_standardize = df.drop(columns=['user', 'agree_to_marketing', 'is_joined_spring', 'is_used_brandpay'])
    scaler = StandardScaler()
    standardized_features = scaler.fit_transform(features_to_standardize)
    standardized_df = pd.DataFrame(standardized_features, columns=features_to_standardize.columns)
    standardized_df['user'] = df['user']
    standardized_df['agree_to_marketing'] = df['agree_to_marketing']
    standardized_df['is_joined_spring'] = df['is_joined_spring']
    standardized_df['is_used_brandpay'] = df['is_used_brandpay']
    return standardized_df

def process_data(input_path):
    # Prepare data
    order= get_master_order(input_path)
    review = get_review(input_path)

    # Select data
    df = order[['user', 'date_joined', 'is_sms', 'is_news', 'last_visited_at', 'claim_count',
                'created_at', 'order_type', 'payment_method', 'product_price', 'product_discount_price',
                'coupon_discount_price', 'point_discount_price', '배송타입', '카테고리', 'species', 'cnt']]
    df['date_joined'] = df['date_joined'].apply(parser.parse, dayfirst=False)
    df['last_visited_at'] = df['last_visited_at'].apply(parser.parse, dayfirst=False)
    df['created_at'] = df['created_at'].apply(parser.parse, dayfirst=False)
    df['discount_price'] = df.apply(calculate_discount_mean, axis=1)

    # Create features (except reviews) used in clustering
    user_data = df.groupby('user').agg(
        purchase_counts=('cnt', 'max'),
        purchase_price_mean=('product_price', 'mean'),
        purchase_price_sum=('product_price', 'sum'),
        discount_price_mean=('discount_price', 'mean'),
        days_since_joined=('date_joined', lambda x: days_between(x.iloc[0], datetime.now())),
        days_since_last_visit=('last_visited_at', lambda x: days_between(x.iloc[0], datetime.now())),
        claim_count=('claim_count', 'max'),
        agree_to_marketing=('is_sms', 'mean'),
        is_news=('is_news', 'mean'),
        is_joined_spring=('date_joined', lambda x: int(x.iloc[0].month in [3, 4, 5])),
        is_used_brandpay=('payment_method', lambda x: int('brandpay' in x.values)),
        num_species=('species', lambda x: x.nunique() if x.notna().any() else 0),
        spring_order_prop=('created_at', lambda x: (x.dt.month.isin([3, 4, 5]).sum() / len(x))),
        lunch_order_prop=('created_at', lambda x: ((x.dt.hour >= 3) & (x.dt.hour < 15)).sum() / len(x)),
        regular_prop=('order_type', lambda x: (x == 'regular').sum() / len(x)),
        morning_prop=('배송타입', lambda x: (x == 'morning').sum() / len(x)),
        single_flower_prop=('카테고리', lambda x: (x == 1).sum() / len(x)),
        add_ons_prop=('카테고리', lambda x: (x == 0).sum() / len(x))
    ).reset_index()
    user_data['days_per_purchase'] = user_data['days_since_joined'] / user_data['purchase_counts']
    user_data['agree_to_marketing'] = (user_data['agree_to_marketing'] + user_data['is_news']) / 2
    user_data = user_data.drop(columns=['is_news'])

    # Standardize data
    standardized_df= standardized_df(user_data)
    
    # Create features concerning reviews used in clustering
    user_review_counts = review.groupby('user id').size().reset_index(name='review_count').rename(columns={'user id': 'user'})
    user_order_counts = standardized_df[['user', 'purchase_counts']].rename(columns={'user': 'user'})
    user_review_ratio = pd.merge(user_review_counts, user_order_counts, on='user', how='right')
    user_review_ratio['review_ratio'] = user_review_ratio['review_count'] / user_review_ratio['purchase_counts']
    user_review_ratio['review_ratio'] = user_review_ratio['review_ratio'].fillna(0)
    user_avg_rating = review.groupby('user id')['총점'].mean().reset_index(name='average_rating').rename(columns={'user id': 'user'})
    user_review_features = pd.merge(user_review_counts, user_avg_rating, on='user', how='right')
    user_review_features = pd.merge(user_review_features, user_review_ratio[['user', 'review_ratio']], on='user', how='right')
    user_review_features['review_count'] = user_review_features['review_count'].fillna(0)
    user_review_features['average_rating'] = user_review_features['average_rating'].fillna(0)
    user_grouped_standardized = pd.merge(standardized_df, user_review_features, on='user', how='left')
    user_grouped_standardized = user_grouped_standardized.fillna(0)
    user_grouped_standardized['review_group'] = user_grouped_standardized.apply(categorize_review_count, axis=1)
    user_grouped_standardized = user_grouped_standardized.drop(columns=['review_count', 'average_rating', 'review_ratio'])
    
    return user_grouped_standardized