import json
import pymysql
import pyproj
from pyproj import Transformer

# 좌표계 변환 함수
def tm_to_wgs84(x, y):
    tm_rs = pyproj.CRS("+proj=tmerc +lat_0=38 +lon_0=127.0028902777778 +k=1 +x_0=200000 +y_0=500000 +ellps=bessel +units=m +no_defs +towgs84=-115.80,474.99,674.11,1.16,-2.31,-1.63,6.43") # 서울 열린데이터광장 x,y 좌표계 기준 : TM(EPSG:2097) # 정확한 좌표값은 ESPG:5178
    wgs84 = pyproj.CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ")  # WGS 84
    # CRS값 참조 : http://www.gisdeveloper.co.kr/?p=8942
    transformer = pyproj.Transformer.from_crs(tm_rs, wgs84, always_xy=True)
    longitude, latitude = transformer.transform(x, y)
    return latitude, longitude

# JSON 데이터 로드
with open('./data/서울시 일반음식점 인허가 정보.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# config 파일 로드
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# MySQL 연결 설정
connection = pymysql.connect(
    host=config['db_host'],
    user=config['db_user'],
    password=config['db_password'],
    db=config['db_name']
)

cursor = connection.cursor()

temp = 0
## 데이터 삽입
try:
    with connection.cursor() as cursor:
        # Loop through the data and insert records into the restaurant table
        for record in data['DATA']:
            latitude, longitude = tm_to_wgs84(record['x'], record['y'])
            input_data = {
                'address': record['sitewhladdr'],
                'lat': latitude,
                'license_dttm': record['apvpermymd'],
                'lon': longitude,
                'name': record['bplcnm'],
                'rest_type': record['uptaenm'],
                'state': record['trdstatenm'],
                'postal_code': record['sitepostno'],
                'homepage': record['homepage'],
                'tm_x': float(record['x']),
                'tm_y': float(record['y']),
            }
            
            insert_query = """
            INSERT INTO restaurant (address, lat, license_dttm, lon, name, rest_type, state, postal_code, homepage, tm_x, tm_y)
            VALUES (%(address)s, %(lat)s, %(license_dttm)s, %(lon)s, %(name)s, %(rest_type)s, %(state)s, %(postal_code)s, %(homepage)s, %(tm_x)s, %(tm_y)s)
            """
            cursor.execute(insert_query, input_data)
            connection.commit()
            
        # Commit the changes
        connection.commit()

finally:
    # Close the connection
    connection.close()


## 데이터 형식
"""
"DESCRIPTION": {
    "ISREAM": "보증액",
    "JTUPSOMAINEDF": "전통업소주된음식",
    "FCTYPDTJOBEPCNT": "공장생산직종업원수",
    "SITEPOSTNO": "소재지우편번호",
    "MONAM": "월세액",
    "TOTEPNUM": "총인원",
    "UPTAENM": "업태구분명",
    "FCTYSILJOBEPCNT": "공장판매직종업원수",
    "LASTMODTS": "최종수정일자",
    "HOFFEPCNT": "본사종업원수",
    "CLGENDDT": "휴업종료일자",
    "UPDATEGBN": "데이터갱신구분",
    "RDNWHLADDR": "도로명주소",
    "DCBYMD": "폐업일자",
    "SITEWHLADDR": "지번주소",
    "Y": "좌표정보(Y)",
    "HOMEPAGE": "홈페이지",
    "DTLSTATEGBN": "상세영업상태코드",
    "TRDSTATEGBN": "영업상태코드",
    "X": "좌표정보(X)",
    "OPNSFTEAMCODE": "개방자치단체코드",
    "APVPERMYMD": "인허가일자",
    "JTUPSOASGNNO": "전통업소지정번호",
    "FCTYOWKEPCNT": "공장사무직종업원수",
    "WMEIPCNT": "여성종사자수",
    "UPDATEDT": "데이터갱신일자",
    "BPLCNM": "사업장명",
    "WTRSPLYFACILSENM": "급수시설구분명",
    "SNTUPTAENM": "위생업태명",
    "CLGSTDT": "휴업시작일자",
    "RDNPOSTNO": "도로명우편번호",
    "ROPNYMD": "재개업일자",
    "MGTNO": "관리번호",
    "FACILTOTSCP": "시설총규모",
    "MULTUSNUPSOYN": "다중이용업소여부",
    "LVSENM": "등급구분명",
    "TRDSTATENM": "영업상태명",
    "SITEAREA": "소재지면적",
    "TRDPJUBNSENM": "영업장주변구분명",
    "SITETEL": "전화번호",
    "APVCANCELYMD": "인허가취소일자",
    "BDNGOWNSENM": "건물소유구분명",
    "DTLSTATENM": "상세영업상태명",
    "MANEIPCNT": "남성종사자수"
  },
"""

# 데이터 예시
"""
{
    "lastmodts": "2023-03-31 15:18:13",
    "dtlstatenm": "영업",
    "totepnum": null,
    "wmeipcnt": null,
    "bplcnm": "랩보이(wrap boy)",
    "maneipcnt": null,
    "isream": null,
    "jtupsoasgnno": null,
    "faciltotscp": null,
    "jtupsomainedf": null,
    "multusnupsoyn": null,
    "clgenddt": "",
    "sitearea": "27.70",
    "dcbymd": "",
    "clgstdt": "",
    "trdstategbn": "01",
    "trdstatenm": "영업/정상",
    "apvcancelymd": "",
    "sitepostno": "135-933",
    "fctysiljobepcnt": null,
    "opnsfteamcode": "3220000",
    "sitetel": "",
    "fctypdtjobepcnt": null,
    "sitewhladdr": "서울특별시 강남구 역삼동 823 삼원타워",
    "dtlstategbn": "01",
    "rdnpostno": "06234",
    "bdngownsenm": null,
    "trdpjubnsenm": null,
    "homepage": null,
    "monam": null,
    "fctyowkepcnt": null,
    "updategbn": "I",
    "updatedt": 1670079720000,
    "apvpermymd": "2023-03-31",
    "wtrsplyfacilsenm": null,
    "lvsenm": null,
    "uptaenm": "기타",
    "hoffepcnt": null,
    "rdnwhladdr": "서울특별시 강남구 테헤란로 124, 삼원타워 지하1층 123호 (역삼동)",
    "sntuptaenm": null,
    "y": "444055.07069569",
    "ropnymd": "",
    "mgtno": "3220000-101-2023-00456",
    "x": "202722.53183055"
},
"""

## 데이터 출처 : http://data.seoul.go.kr/dataList/OA-16094/A/1/datasetView.do