import json
import pymysql

# JSON 데이터 로드
with open('./data/서울시 일반음식점 인허가 정보.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# MySQL 연결 설정
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='1234',
    db='tmi'
)

cursor = connection.cursor()

## 데이터 삽입
try:
    with connection.cursor() as cursor:
        # Loop through the data and insert records into the restaurant table
        for record in data['DATA']:
            sql = """INSERT INTO restaurant (address, lat, license_dttm, lon, name, rest_type, start_dttm)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            # x, y 좌표 UTM -> WGS84 좌표변환 필요 || 테이블에 UTM 좌표저장 컬럼 추가 필요
            cursor.execute(sql, (record['rdnwhladdr'], record['y'], record['apvpermymd'], record['x'], record['bplcnm'], record['uptaenm'], record['updatedt']))

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