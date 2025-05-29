from data import process_data, get_user
from argparse import ArgumentParser
from sklearn.cluster import KMeans
import pandas as pd


def main(args):
    # Prepare Data
    data= process_data(args.input_path)
    user= get_user(args.input_path)

    # Clustering
    data_for_clustering = data[['purchase_counts', 'purchase_price_mean', 'discount_price_mean',
                                           'days_since_joined', 'days_since_last_visit', 'agree_to_marketing',
                                           'spring_order_prop', 'lunch_order_prop', 'regular_prop', 'review_group']]
    optimal_k = 5
    kmeans = KMeans(n_clusters=optimal_k, random_state=42)
    clusters = kmeans.fit_predict(data_for_clustering)
    data['cluster'] = clusters

    # Save a result
    user_clusters = pd.merge(user, data[['user', 'review_group', 'cluster']], on='user', how='left')
    user_clusters.to_csv(args.output_path + "/cluster_result.csv", index= None)
    print("The result has saved.")


if __name__ == "__main__":
    parser = ArgumentParser()

    # Specify input_path of server data
    parser.add_argument("--input_path", type= str, required= True)

    # Specify output_path for saving the result of clustering
    parser.add_argumet("--output_path", type= str, required= True)
    args = parser.parse_args()

    # use terminal: python main.py --input_path {INPUT_PATH} --output_path {OUTPUT_PATH}
    main(args)