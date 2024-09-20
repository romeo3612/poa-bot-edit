from pocketbase import PocketBase
import jwt
from exchange.utility import log_message, log_error_message, settings
import time
import traceback
from datetime import datetime, timedelta


pb = PocketBase("http://127.0.0.1:8090")


def auth():
    try:
        DB_ID = settings.DB_ID
        DB_PASSWORD = settings.DB_PASSWORD
        pb.admins.auth_with_password(DB_ID, DB_PASSWORD)
    except Exception as e:
        raise Exception("DB auth error")


def reauth():
    try:
        token = pb.auth_store.base_token
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        expire_time = decoded_token["exp"]
        current_time = int(time.time())
        if current_time > expire_time:
            auth()
    except:
        raise Exception("DB reauth error")

 
def create(collection, data):
    try:
        reauth()
        pb.collection(collection).create(data)
    except:
        raise Exception("DB create error")


def delete(collection, id):
    try:
        reauth()
        pb.collection(collection).delete(id)
    except:
        raise Exception("DB delete error")



def get_full_list(collection, batch_size=200, query_params=None):
    try:
        print(f"DEBUG: get_full_list 호출 - collection: {collection}, batch_size: {batch_size}, query_params: {query_params}")
        reauth()
        return pb.collection(collection).get_full_list(
            batch=batch_size, query_params=query_params
        )
    except Exception as e:
        print(f"DEBUG: PocketBase에서 get_full_list 호출 중 오류 발생 - {str(e)}")
        raise Exception("DB get_full_list error")


"""
def get_full_list(collection, batch_size=200, query_params=None):
    try:
        reauth()
        return pb.collection(collection).get_full_list(
            batch=batch_size, query_params=query_params
        )
    except:
        raise Exception("DB get_full_list error")
"""

def delete_old_records():

    try:
        reauth()
        one_year_ago = datetime.now() - timedelta(days=365)
        query = f"timestamp <= '{one_year_ago.strftime('%Y-%m-%dT%H:%M:%S')}'"
        old_records = pb.collection("pair_order_history").get_full_list(query_params={"filter": query})
        
        for record in old_records:
            delete("pair_order_history", record.id)

    except:
        raise Exception("DB delete_old_records error")



try:
    auth()
except:
    log_error_message(traceback.format_exc(), "DB auth error")
