# py 명령어로 실행
import csv # csv 파일을 읽어오기 위함
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="my_application")

with open('./csv/서울특별시_성동구_일반음식점현황_20220818.csv') as csvfile:
    reader = csv.reader(csvfile)
    i = 0
    for row in reader:
        # type = row[0] # 업종명
        date_of_license = row[1]    # 인허가일자
        restaurant_name = row[2]    # 업소명
        address = row[3]            # 도로명주소
        # row[4]                    # 영업장면적
        start_date = row[5]         # 영업자시작일
        restaurant_type = row[6]    # 업태명
        
        address = address.split(" (")[0] # (동이름) 으로 인해서 오류 발생하기 때문에 잘라준다
        location = geolocator.geocode(address)
        if location is not None:
            latitude = location.latitude
            longitude = location.longitude
        else:
            latitude = None
            longitude = None
        
        obj = {
            "license_dttm": date_of_license,
            "name" : restaurant_name,
            "address" : address,
            "start_dttm" : start_date,
            "rest_type" : restaurant_type,
        }
        print(obj)
        # db 저장 로직 필요
        i+=1
        if(i==10):
            break
    