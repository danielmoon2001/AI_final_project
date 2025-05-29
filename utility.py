# file for helper functions

def days_between(d1, d2):
    return (d2 - d1).days

def calculate_discount_mean(row):
    return row['product_discount_price'] + row['coupon_discount_price'] + row['point_discount_price']

def categorize_review_count(row):
    if row['review_count'] == 0:
        return 0
    elif 1 <= row['review_count'] <= 2:
        return 1
    else:
        return  2
    
def convert_list_to_string(value):
    if isinstance(value, list):
        return ",".join(str(x) for x in value)
    else:
        return value