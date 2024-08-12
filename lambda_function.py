import os
import json
import time
from itertools import count
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from openai import OpenAI
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

def lambda_handler(event, context):
    
    headers = None
    rows = []
    for page in count(1):
        print('try to page = {}'.format(page))
        data = get_page(page)
        if not data['rows']:
            print('페이지 끝에 도달')
            break
        if headers is None:
            headers = data['headers']
        rows.extend(data['rows'])
        time.sleep(0.5)
    
        if page >= 2:
            print('2페이지까지만 진행하겠습니다.')
            break
    
    """ Open API """
    result = query_openai(rows)
    """ Slack API """
    res = send_msg(result)
    return {
        'statusCode': 200,
        'body': json.dumps(f'{res}')
    }



def get_page(page):
    st_dt = datetime.now()
    st_month = f"0{st_dt.month}" if st_dt.month < 10 else f"{st_dt.month}"
    end_dt = st_dt - timedelta(days=30) # 한달 전 공고까지
    year = f"{end_dt.year}"
    month = f"0{end_dt.month}" if end_dt.month < 10 else f"{end_dt.month}"
    day = f"{end_dt.day}"

    url = f"https://www.g2b.go.kr:8101/ep/tbid/tbidList.do?searchType=1&bidSearchType=1&taskClCds=5&taskClCds=20&bidNm=&searchDtType=1&fromBidDt={year}/{month}/{day}&toBidDt={st_dt.year}/{st_month}/{st_dt.day}&fromOpenBidDt=&toOpenBidDt=&radOrgan=1&instNm=&instSearchRangeType=&refNo=&area=&areaNm=&strArea=&orgArea=&industry=&industryCd=&upBudget=&downBudget=&budgetCompare=&detailPrdnmNo=&detailPrdnm=&procmntReqNo=&intbidYn=&regYn=Y&recordCountPerPage=30"
   
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    table_tag = soup.select('#resultForm > div.results > table')[0]

    headers = [th_tag.text for th_tag in table_tag.select('thead > tr > th')]
    headers.append("사업금액(추정가격+부가세)")
    rows = []
    for tr_tag in table_tag.select('tbody > tr'):
        cols = [td_tag.find("a").get('href') if td_tag.find("a") and tr_tag.select('td').index(td_tag) == 1 else td_tag.text for td_tag in tr_tag.select('td')]

        """ 세부 가격 찾기"""
        try:
            sp = BeautifulSoup(requests.get(cols[1]).text, 'html.parser')
            
            price = sp.find(string="(추정가격 + 부가세)").parent.parent.find_next_sibling().select('div')[0].text.replace(" ","").replace("\r", "").replace("\n", "").replace("\t", "")
            cols.append(price)
        except:
            cols.append(0)
        if len(cols) > 1:
            rows.append(cols)
    
    return {
        'headers': headers,
        'rows': rows,
    }
    

def query_openai(rows):
    client = OpenAI(
        api_key = os.environ["OPENAI_TOKEN"]
    )
    
    completion = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
    {"role": "system", "content": """As the head of the business planning team, you must select the relevant service from the list of service bids for the National Marketplace in Korea. At this time, our team’s capabilities are as follows.
1. Development services: advancement, service, development, advancement, platform, portal, system, maintenance
2. Marketing services: YouTube, public relations, marketing, SNS, video, online channel operation
3. Please sort out the ones worth less than 150 million won and Please extract as much data as possible.
* The headers of CSV data are [task (classification), URL, classification, announcement name, announcement agency, demand agency, contract method, input date (bidding deadline date and time), joint supply, bidding, deposit amount].
You need to find it in the service list based on keywords. Separate and organize each development and marketing service.
The Result format is below, 

Part: SW개발 or 마케팅
1. {title}
- 가격: {Price}
- 공고기관: {Demand Agency}
- 제출 마감일: {Deadline Date}
- URL: URL_PATH

----------------------------------------- this line is divider

    """},
        {"role": "user", "content": f"""List of CSV data, 
        {rows}
        """}
      ]
    )
    return completion.choices[0].message.content
    
def send_msg(txt):
    try:
        client = WebClient(token=os.environ["SLACK_TOKEN"])
        result = client.chat_postMessage(
            channel = os.environ["SLACK_CHANNEL"],
            text = txt
        )
        return result
    except SlackApiError as e:
        return f"Error posting message: {e}"
