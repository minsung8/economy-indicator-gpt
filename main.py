from typing import Union

from fastapi import FastAPI
import requests

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bson.json_util import dumps


app = FastAPI()
scheduler = AsyncIOScheduler()

# 배치로 실행할 함수 정의
async def scheduled_job():
    update_data()
    print(f"Batch job executed at {datetime.now()}")


# 애플리케이션 시작 시 스케줄러 시작 및 작업 추가
@app.on_event("startup")
async def start_scheduler():
    scheduler.add_job(scheduled_job, "cron", hour=0, minute=0)  # 매일 자정에 실행
    scheduler.start()


def get_data():
    # 오늘 날짜 얻기 (datetime.today() 메서드 사용)
    today = datetime.today()

    # 내일 날짜 계산
    tomorrow = today + timedelta(days=1)

    # 날짜를 'YYYY-MM-DD' 형식으로 포매팅
    start_date = today.strftime('%Y-%m-%d')
    end_date = tomorrow.strftime('%Y-%m-%d')

    url = f"CRAWLING_URL"

    data = requests.get(url=url).json()

    count = len(data['kevent'])

    temp_list = []
    for i in range(count):
        temp = {}
        temp['경제지표'] = data['kevent'][i]
        temp['이전'] = data['previous'][i]
        temp['예상'] = data['forecast'][i]
        temp['실제'] = data['actual'][i]
        temp['중요도'] = data['importance'][i]

        temp_list.append(temp)

    return {start_date: temp_list}

@app.get("/")
def read_root():
    return {"Hello": "World2"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/crawling")
def update_data():
    uri = "DB_URL"
    # Create a new client and connect to the server
    client = MongoClient(uri)

    # Send a ping to confirm a successful connection
    try:
        db = client['test']

        # 컬렉션 선택 (없으면 자동 생성)
        collection = db['test']

        # 데이터 생성 (document 추가)

        post = get_data()

        post_id = collection.insert_one(post).inserted_id
        print("Post ID:", post_id)
    except Exception as e:
        print(e)

    return {"result": True}

@app.get("/data/{date}")
def get_data_by_date(date: str):
    uri = "DB_URL"
    client = MongoClient(uri)
    db = client['test']
    collection = db['test']

    # Assuming the date in the MongoDB documents is stored in ISO format or similar
    # and the API receives the date in 'YYYY-MM-DD' format.
    try:
        # Convert the input date string to a datetime object
        query_date = datetime.strptime(date, "%Y-%m-%d")

        # Query MongoDB for documents on this date
        # Adjust the query according to how your dates are stored (e.g., as a string, ISODate, etc.)
        documents = collection.find({"start_date": query_date})


        # Convert the query result to a list and then to JSON
        result = dumps(list(documents))
        return result

    except ValueError as ve:
        # If the date format is incorrect
        return HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")
    except errors.PyMongoError as e:
        # Handle MongoDB errors
        return HTTPException(status_code=500, detail="Could not fetch data from MongoDB.")
    finally:
        client.close()
