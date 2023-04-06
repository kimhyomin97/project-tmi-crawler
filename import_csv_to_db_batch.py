import requests
import csv
import mysql.connector
from datetime import datetime
import requests

def get_location(address):
    # 카카오 API 호출을 위한 URL
    url = 'https://dapi.kakao.com/v2/local/search/address.json'
    # 카카오 API 키
    api_key = 'b11ab2030a81e11a7b5597898a0f7b21'
    # 카카오 API 호출
    response = requests.get(url, headers={'Authorization': f'KakaoAK {api_key}'}, params={'query': address})
    # 좌표 추출
    try:
        result = response.json()['documents'][0]
    except (KeyError, IndexError):
        return None
    
    return result['y'], result['x']

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="tmi"
)

mycursor = mydb.cursor()

# CSV 파일에서 데이터 읽어오기
with open('./csv/서울특별시_성동구_일반음식점현황_20220818.csv') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) # 첫번째 줄은 헤더이므로 skip
    rows = []
    for row in reader:
        date_of_license = datetime.strptime(row[1], '%Y-%m-%d').date() # 인허가일자
        restaurant_name = row[2] # 업소명
        address = row[3] # 도로명주소
        start_date = datetime.strptime(row[5], '%Y-%m-%d').date() # 영업자시작일
        restaurant_type = row[6] # 업태명
        rows.append((date_of_license, restaurant_name, address, start_date, restaurant_type))

# 주소를 좌표로 변환하여 데이터베이스에 저장하기
sql = "INSERT INTO restaurant (license_dttm, name, address, start_dttm, rest_type, lat, lon) VALUES (%s, %s, %s, %s, %s, %s, %s)"
vals = []
for row in rows:
    address = row[2]
    address = address.split(" (")[0]
    if not address:
        continue
    else:
        location = get_location(address)
        if location is not None:
            latitude, longitude = location
        else:
            latitude, longitude = None, None
        vals.append((row[0], row[1], row[2], row[3], row[4], latitude, longitude))
        # print((row[0], row[1], row[2], row[3], row[4], latitude, longitude))

mycursor.executemany(sql, vals)
mydb.commit()
