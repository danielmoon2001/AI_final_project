import pandas as pd
from data import get_user
from utility import convert_list_to_string

def user_grouporder_merge(input_path):
    user= get_user(input_path)

    # Select homepage orders only in '그룹주문서'
    ordergroup= pd.read_csv(input_path + "/grouporder.csv")
    homepage_ordergroup = ordergroup[ordergroup['channel'].isin(['homepage']) & (ordergroup['user_type'] == 'user')]
    homepage_ordergroup = homepage_ordergroup[(homepage_ordergroup['user'].notnull())]
    homepage_ordergroup.reset_index(drop= True, inplace= True)
    homepage_ordergroup= homepage_ordergroup[['number', 'user',
                                          'status', 'created_at',
                                          'payment', 'order_type', 'channel', 'regular_order'
                                          ]]
    homepage_ordergroup['number']= homepage_ordergroup['number'].astype(str)
    homepage_ordergroup['user']= homepage_ordergroup['user'].astype(int).astype(str)
    homepage_ordergroup['status']= homepage_ordergroup['status'].astype(str)
    homepage_ordergroup['created_at']= homepage_ordergroup['created_at'].astype('datetime64[ns]')
    homepage_ordergroup['payment']= homepage_ordergroup['payment'].astype(int).astype(str)
    homepage_ordergroup['order_type']= homepage_ordergroup['order_type'].astype(str)
    homepage_ordergroup['channel']= homepage_ordergroup['channel'].astype(str)
    # 결측치 처리 확인 必
    homepage_ordergroup['regular_order'].fillna('0', inplace= True)
    homepage_ordergroup['regular_order']= homepage_ordergroup['regular_order'].astype(int).astype(str)
    homepage_ordergroup.rename(columns= {'status':'grouporder_status'}, inplace= True)
    homepage_ordergroup.drop_duplicates(inplace= True)
    
    # Merge '고객' and '그룹주문서'
    user= user['id', 'brandpay_joined', 'date_joined', 'left_at', 'invitation_code',
       'is_sms', 'is_news', 'last_visited_at', 'claim_count',
       'birth', 'flower_taste']
    user['id']= user['id'].astype(str)
    user['last_visited_at']= user['last_visited_at'].astype('period[D]')
    user['date_joined']= user['date_joined'].astype('period[D]')
    # 결측치 처리 확인 必
    user['left_at'].fillna('2099-12-31', inplace= True)
    user['left_at']= user['left_at'].astype('period[D]')
    # 결측치 처리 확인 必
    user['brandpay_joined'].fillna('2099-12-31', inplace= True)
    user['brandpay_joined']= user['brandpay_joined'].astype('period[D]')
    # 결측치 처리 확인 必
    user['birth'].fillna('2099-12-31', inplace= True)
    user['birth']= user['birth'].astype('period[D]')
    # 결측치 처리 확인 必
    user['invitation_code'].fillna('unknown', inplace= True)
    user['invitation_code']= user['invitation_code'].astype(str)
    # 결측치 처리 확인 必
    user['flower_taste'].fillna('unknown', inplace= True)
    user['flower_taste']= user['flower_taste'].apply(lambda x: x.split(','))

    user_homepage= homepage_ordergroup.merge(right= user, left_on= 'user', right_on='id', how= 'left')
    user_homepage.drop(['id'], axis= 1, inplace= True)

    for column in user_homepage.columns:
        if user_homepage[column].apply(lambda x: isinstance(x, list)).any():
            user_homepage[column] = user_homepage[column].apply(convert_list_to_string)

    user_homepage= user_homepage[['user', 'user_type', 'brandpay_joined',
                              'date_joined', 'left_at', 'invitation_code', 'is_sms', 'is_news',
                              'last_visited_at', 'claim_count', 'birth',
                              'flower_taste', 'number', 'created_at', 'grouporder_status', 'payment', 'order_type', 'channel', 'reward_point', 'regular_order',
                              ]]

    return user_homepage


def add_payment(input_path, user_grouporder):
    payment= pd.read_csv(input_path + "/payment.csv")
    cleared_payment= payment.drop(['주문일', '주문년월', 'pg', '정기/일반', '결제수단', '이메일', '전화번호', '가입일', '가입년월', '한달 이내 구매여부', 'channel', 'channel_detail', '아이템 수', 'tax_free', 'tax_free_cancelled', '정산가', 'amount_paid', 'discount_price', 'amount_cancelled', 'amount_refunded'], axis= 1)
    cleared_payment.rename(columns= {'id':'payment', '결제방식':'payment_method', 'status': 'payment_status'}, inplace= True)
    cleared_payment['payment']= cleared_payment['payment'].astype(str)

    cleared_payment['payment_method']= cleared_payment['payment_method'].astype(str)
    cleared_payment['payment_status']= cleared_payment['payment_status'].astype(str)
    cleared_payment.drop('payment', axis= 1, inplace= True)

    user_grouporder_payment= user_grouporder.merge(right= cleared_payment, how= 'left', on= 'payment')
    return user_grouporder_payment

def add_orderitem(input_path, user_grouporder_payment):
    item= pd.read_csv(input_path + "/item.csv")
    cleared_item= item.drop(['id', '결제일', '결제시간', '외부채널 주문번호', '정산 가격 (수량 * 1개당 정산가)', '농가정산비율', '(구)주문서', '이름', '주문서 제목', '채널', '외부채널 상세', '주문서 타입', '이메일', '휴대폰'], axis= 1)
    cleared_item.rename(columns= {'그룹주문번호':'number'}, inplace= True)

    cleared_item['number']=cleared_item['number'].astype(str)
    cleared_item['주문번호']=cleared_item['주문번호'].astype(str)
    cleared_item['상태']=cleared_item['상태'].astype(str)
    cleared_item['농부']=cleared_item['농부'].astype(str)
    cleared_item['상품 상세 SKU']=cleared_item['상품 상세 SKU'].astype(str)
    cleared_item['상품 상세 명']=cleared_item['상품 상세 명'].astype(str)
    cleared_item['수령일']=cleared_item['수령일'].astype('period[D]')
    cleared_item['가격']=cleared_item['가격'].astype('int')
    cleared_item['수량']=cleared_item['수량'].astype('int')
    cleared_item['환불금액']=cleared_item['환불금액'].astype('int')

    cleared_item.drop_duplicates(inplace= True)

    final= user_grouporder_payment.merge(right= cleared_item, how= 'left', on= 'number')
    drop_final = final[final['상품 상세 명'].notna()]
    drop_final = drop_final[drop_final['배송타입'].notna()]
    return drop_final

def get_merged_df(input_path):
    user_grouporder= user_grouporder_merge(input_path)
    user_grouporder_payment= add_payment(input_path, user_grouporder)
    final= user_grouporder_payment.add_orderitem(input_path, user_grouporder_payment)
    return final

def rearrange_date_joined(merged):
    # 가입일이 첫 구매일보다 늦은 고객 존재. 이 경우 가입일을 첫 구매일로 지정.
    interval_df= merged.copy()
    interval_df['date_joined']= pd.to_datetime(interval_df['date_joined'].str.slice(0,10))
    interval_df['created_at']= pd.to_datetime(interval_df['created_at'])
    interval_df['first_buying'] = interval_df.groupby('user')['created_at'].transform('min')
    interval_df= interval_df[['user', 'first_buying', 'date_joined']]
    interval_df.drop_duplicates(inplace= True)
    interval_df['interval']= (interval_df['first_buying']-interval_df['date_joined']).dt.days
    user_list = interval_df[interval_df['interval'] < 0]['user'].tolist()
    df_update = pd.merge(merged, interval_df[['user', 'first_buying']], on='user', how='left')
    df_update.loc[df_update['user'].isin(user_list), 'date_joined'] = df_update.loc[df_update['user'].isin(user_list), 'first_buying']
    df_update.drop(columns=['first_buying'], inplace=True)
    return df_update

def make_realorder(rearranged):
    # delete CS, cancelled, refunded, ...
    cs= rearranged[rearranged['주문번호'].str[-2:] == 'CS']
    cs_index= cs.index.tolist()
    cs['key'] = cs['user'].astype(str) + '-' + cs['created_at']
    withoutcs_df = rearranged.drop(cs_index, axis= 0)
    withoutcs_df['key'] = withoutcs_df['user'].astype(str) + '-' + withoutcs_df['created_at']

    matching_rows = cs[cs['key'].isin(withoutcs_df['key'])]
    withoutcs_df.loc[withoutcs_df['key'].isin(cs['key']), 'CS'] = 1
    withoutcs_df.drop(['key'], axis= 1, inplace= True)
    cs.drop(['key'], axis= 1, inplace= True)
    
    unmatching_rows = cs[~cs['key'].isin(withoutcs_df['key'])]
    unmatching_rows.drop(['key'], axis=1, inplace= True)
    unmatching_rows['CS'] = 1

    after_cs= pd.concat([withoutcs_df, unmatching_rows])
    after_cs.loc[after_cs['claim 상태'].notnull(), 'CS']= 1
    after_cs.loc[(after_cs['grouporder_status'] == 'closed') & (after_cs['payment_status'] == 'cancelled') & (after_cs['주문서 상태'] == '배송완료'), 'CS']= 1
    after_cs.loc[(after_cs['grouporder_status'] == 'closed') & (after_cs['payment_status'] == 'refunded') & (after_cs['주문서 상태'] == '배송완료'), 'CS']= 1
    after_cs.loc[(after_cs['grouporder_status'] == 'paid') & (after_cs['payment_status'] == 'paid') & (after_cs['주문서 상태'] == '결제취소'), 'CS']= 1

    after_cs.loc[(after_cs['grouporder_status'] == 'cancelled') & (after_cs['payment_status'] == 'cancelled'), 'CS']= 2
    after_cs.loc[(after_cs['grouporder_status'] == 'cancelled') & (after_cs['payment_status'] == 'paid') & (after_cs['주문서 상태'] == '결제취소'), 'CS']= 2
    after_cs.loc[(after_cs['grouporder_status'] == 'closed') & (after_cs['payment_status'] == 'cancelled') & (after_cs['주문서 상태'] == '결제취소'), 'CS']= 2
    after_cs.loc[(after_cs['grouporder_status'] == 'closed') & (after_cs['payment_status'] == 'paid') & (after_cs['주문서 상태'] == '결제취소'), 'CS']= 2

    df= after_cs.copy()

    condition1 = (df['grouporder_status'] == 'delivered') & (df['payment_status'] == 'paid') & (df['주문서 상태'] == '배송완료')
    condition2 = (df['grouporder_status'] == 'closed') & (df['payment_status'] == 'stop')
    condition3 = df['grouporder_status'] == 'stop'
    condition4 = df['grouporder_status'] == 'prepared'
    condition5 = df['grouporder_status'] == 'paid'
    condition6 = (df['grouporder_status'] == 'closed') & (df['payment_status'] == 'refunded') & (df['주문서 상태'] == '결제취소')

    # Combine conditions - rows matching any condition will be dropped
    drop_mask = condition1 | condition2 | condition3 | condition4 | condition6 | condition5
    # Invert drop_mask to select rows to keep
    keep_mask = ~drop_mask

    # Filter the DataFrame
    filtered_df = df[keep_mask]
    filtered_df['CS'].fillna(0, inplace= True)

    filtered_df.drop(['grouporder_status', 'payment_status', '상태', '주문서 상태', 'key', '환불금액', 'regular_order'], axis= 1, inplace= True)
    realorder= filtered_df[filtered_df['CS'] != 2]
    realorder.drop(['claim 상태', '주문서 메모', '사고접수 메모'], axis= 1, inplace= True)
    return realorder


def get_master_order(input_path):
    merged= get_merged_df(input_path)
    rearranged= rearrange_date_joined(merged)
    order= make_realorder(rearranged)
    return order