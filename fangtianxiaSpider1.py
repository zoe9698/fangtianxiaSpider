#coding='utf-8'
from change_proxy_Test import *
from multiprocessing import Pool
import requests
import re 
import pymysql
import random
import queue
import time
agents = [
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0 x64; en-US; rv:1.9pre) Gecko/2008072421 Minefield/3.0.2pre",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.9.0.11) Gecko/2009060215 Firefox/3.0.11 (.NET CLR 3.5.30729)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 GTB5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; tr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 ( .NET CLR 3.5.30729; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0a2) Gecko/20110622 Firefox/6.0a2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101 Firefox/7.0.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b4pre) Gecko/20100815 Minefield/4.0b4pre",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
    "Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
    "Mozilla/2.02E (Win95; U)",
    "Mozilla/3.01Gold (Win95; I)",
    "Mozilla/4.8 [en] (Windows NT 5.1; U)",
    "Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
]
proxyIP = ()
def change_UserAgent_auto(url):
    host =     (re.compile(r'http://(.*?)/').findall(url))[0]
    headers = {'Host':host,'User-Agent':random.choice(agents)}
    return headers

def update_uselessIPinDB(uselessIP):
    database = pymysql.connect(host="localhost",port=3306,user="zoe",passwd="1235789y",db="proxy_ip_db",charset="utf8")
    cursor = database.cursor()
    sql = 'update proxy_ip set useful=0 where ip_port=%s'
    uselessIP = uselessIP['http'][7:]
    sql_params = (uselessIP,)
    cursor.execute(sql,sql_params)
    database.commit()
    database.close()

def get_proxyIPfromDB():
    #局部变量database
    database = pymysql.connect(host="localhost",port=3306,user="zoe",passwd="1235789y",db="proxy_ip_db",charset="utf8")
    cursor = database.cursor()
    sql = 'select ip_port from proxy_ip where useful=1'
    cursor.execute(sql)
    global proxyIP
    proxyIP = cursor.fetchall()
    database.commit()
    database.close()
 
def change_proxyIP_auto():
    proxy = str(random.choice(proxyIP))[2:-3]
    return {
       'http': 'http://'+proxy,
       'https': 'https://'+proxy,
       }
def get_article(start):
    proxy_ip = change_proxyIP_auto()
    #先单进程的从数据库取数据，一次取1个，放一个长度为8的队列，首次先重复执行8次fetchone把队列填满，
    #接下来多进程执行IO操作，依次取队列中的数据，分给每个进程，IO操作成功将帖子内容存入数据库，再重新从数据库取一个数据放入队列
    #直到遍历完数据库
    IoQue = queue.Queue(9)
    #当队列不满时，sem=true
    sem = False
    conn = pymysql.connect(host="localhost",port=3306,user="zoe",passwd="1235789y",db="fangtianxiadb",charset="utf8")
    cur = conn.cursor(pymysql.cursors.SSCursor)
    sql = 'select a_url,title from ad_article where aid>%s'
    sql_par = (start,)
    print('start:',sql_par)
    cur.execute(sql,sql_par)
    for i in range(9):
        url_title = cur.fetchone()
        IoQue.put(url_title)

    #p = Pool(8)
    while IoQue is not None:
        print('还有帖子未读取，正在读取。')
        #多进程4个，把下面这一块用装饰器包装成函数！！！！！！！！！！！！！！！！！！！
        #for i in range(20):
            #每一波有效ip差不多用完就更新代理ip的DB表，即重新取得10个代理ip
            #if i%(len(proxyIP)*30)==0:
            #    print('@@@@@@@@@@@@@@get_article:每一波有效ip差不多用完就更新代理ip的DB表，即重新取得10个代理ip')
            #    change_proxy1()
            #    get_proxyIPfromDB()        
            #if i%10==0:               
        #def deal():
        url_title = IoQue.get()
        #返回一个信号量，指示队列不满
        sem = True
        comment = ''
        a_url = str(url_title[0])
        title = str(url_title[1]).strip()
        print(a_url)
        print(title)
        i=0           
        while i<3:         
            try:
                print('当前IP：',proxy_ip)
                print('当前帖子：'+a_url)
                headers = change_UserAgent_auto(a_url)
                res = requests.get(a_url,headers=headers,proxies = proxy_ip,timeout=2)
                if(res.status_code!=200):
                    raise Error
                print('该帖子'+'start!!!!')
                print(res.status_code)
                break
            except:            
                print('该代理ip无法进入！',proxy_ip)
                update_uselessIPinDB(proxy_ip)
                get_proxyIPfromDB()
                global proxyIP
                if len(proxyIP)==0:
                    i = i+1
                    print('[get_article]正在更换代理ip时，发现当前数据库中无可用代理ip!')
                    change_proxy1()
                    get_proxyIPfromDB()
                proxy_ip = change_proxyIP_auto()
        if i==3:
            print('这个网址已失效！！！')
            url_title = cur.fetchone()
            IoQue.put(url_title)
            continue
        #soup = BeautifulSoup(res.text,'lxml',from_encoding='utf-8')
        #article_id = (re.compile(r'_(.*?).htm').findall(a_url))[0]
        print('miaomiao~~~~~*8******************')
        while True:
            try:
                #print('res:',res)
                invitations = re.compile(r'div class="invitation">[\s\S]*?<div class="itcom"[\s\S]*?举报</a>[\s]*</div>').findall(res.text)
                #print('invitations:',len(invitations))
                content = re.compile('t_f.*?>(.*?)</td>').findall(invitations[0])
                #print('content1：',len(content))
                if len(content)==0:
                    content = re.compile(r'<div id="HTML_body.*">([\s\S]*?)<br /> <br />').findall(invitations[0])[0]
                    #print(type(content))
                content = str(content).strip()
                #print('content:',content)
                for rubbish in re.compile(r'<[\s\S]*?>').findall(content):
                    #！！！！！replace只是python中字符串是immutable的对象，replace是不会直接变更字符串内容的，只会创建一个新的。需要重新引用将replace返回的替换后的字符串结果。
                    #print(rubbish)
                    content = content.replace(rubbish,' ')
                content = content.replace('&nbsp','').replace(' ','').strip()
                for i in range(1,len(invitations)):
                    cm = re.compile(r'<div id="HTML_body.*">([\s\S]*?)<br /> <br />').findall(invitations[i])[0]
                    try:
                        if '</div>' in str(cm):  
                            print('there is div in it....') 
                            comment = comment + re.compile(r'</div>[\s\S]*?<p>([\s\S]*?)</p>').findall(str(cm))[0]
                        elif '</p>' in str(cm):
                            print('there is p in it....')
                            comment = comment + re.compile(r'<p>([\s\S]*?)</p>').findall(str(cm))[0]
                    except:
                        comment = comment + cm
                    ###########可改进！改成用re模块的sub函数
                comment = comment.strip().replace('<br />','').replace('&nbsp','').replace(' ','').replace('    ','')
                sql = 'insert into content(title,a_url,content,comment) values(%s,%s,%s,%s)'
                sql_params = (title,a_url,content,comment)
                saveDB(sql,sql_params)
                #p.apply_async(deal)         
                url_title = cur.fetchone()
                IoQue.put(url_title)
                break
            except Exception as e:
                print(e)
                print(comment)
                time.sleep(300)    
    cur.close()
    conn.close()
    
    
 
def saveDB(sql,sql_params):
    database = pymysql.connect(host="localhost",port=3306,user="zoe",passwd="1235789y",db="fangtianxiadb",charset="utf8")
    cursor = database.cursor()
    try:
        cursor.execute(sql,sql_params)
        database.commit()
        database.close()
    except pymysql.err.Error as err:
        #如果发生错误就回滚
        print(err,'error sql')
        database.rollback()
        
if __name__ == '__main__':
    change_proxy1()
    get_proxyIPfromDB()
    f = open('E:/社区评分/start.txt')
    start = f.read()
    f.close()
    print(start)
    try:
        get_article(start)
    except KeyboardInterrupt:
        sql = 'select cid from content where a_url=%s'
        sql_par = (a_url)
        database = pymysql.connect(host="localhost",port=3306,user="zoe",passwd="1235789y",db="fangtianxiadb",charset="utf8")
        cursor = database.cursor()
        try:
            cursor.execute(sql,sql_par)
            start = cursor.fetchone()
            database.commit()
            database.close()
        except pymysql.err.Error as err:
            #如果发生错误就回滚
            print(err,'error sql')
            database.rollback()
        f = open('E:/社区评分/start.txt')
        f.write(start)
        f.close()
