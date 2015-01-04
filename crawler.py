#-*- coding:utf-8 -*-
import urllib2
import re
import copy
import os,sys
import time
import random
import datetime
from bs4 import BeautifulSoup
#from http.server import HTTPServer,BaseHTTPRequestHandler
#from apscheduler.scheduler import Scheduler
#import redis
from conf import *  

class crawler:
    #software structure is the same with game structure
    def __init__(self,sources):
        self.req_conf = {
            'SOFTWARE' : {
                'host' : 'http://shouji.baidu.com',
                'url'  : 'http://shouji.baidu.com/software',
                'url_attr' : {
                    'href'   : re.compile(r'/software/list\?cid=\d+'),
                    'title'  : None,
                    'target' : None
                },
                'sec_url_attr' : {
                    'href'   : re.compile(r'^/software/list\?cid=\d+&boardid=board_\d+_\d+$'),
                    'title'  : None,
                    'target' : None
                }
            },
            'GAME' :{
                'host' : 'http://shouji.baidu.com',
                'url'  : 'http://shouji.baidu.com/game',
                'url_attr' : {
                    'href'   : re.compile(r'^/game/list\?cid=\d+$'),
                    'title'  : None,
                    'target' : None
                },
                'sec_url_attr' : {
                    'href'   : re.compile(r'^/game/list\?cid=\d+&boardid=board_\d+_\d+$'),
                    'title'  : None,
                    'target' : None
                }
            },
            'headers' : {
                "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-cn",
                "X-Requested-With" : "XMLHttpRequest"
            }
        }
        self.tgt_time_attr = {
            'class' : re.compile('^title_10\w*$') 
        }
        self.job_sources = sources
        self.first_flag = True

    '''
    sleep interval [0.5, 5.5]
    '''
    def get_sleep_interval(self):
        rand = random.random() * 5 + 0.5
        return rand

    def get_all_classes(self, sec_url, first_id, sec_id, first_class, sec_class, cate, source):
        tgt_arts = []
        req = urllib2.Request(sec_url,headers=self.req_conf['headers'])
        rep = urllib2.urlopen(req)
        ul_tags = BeautifulSoup(rep.read().decode('utf-8')).find_all('div', { "class" : "pager" })[0]
        pagenum = int(ul_tags.attrs[u'data-total'])
        sec = self.get_sleep_interval()
        time.sleep(sec)
        print >> sys.stderr, "sleep " , sec , "s"
        if pagenum <= 1:
            self.get_page_content(sec_url, first_id, sec_id, first_class, sec_class, cate, source)
        else:
            for i in range(1, pagenum + 1):
                if i == 1:
                    url = sec_url
                else:
                    url = sec_url + "&page_num=" + str(i)
                self.get_page_content(url, first_id, sec_id, first_class, sec_class, cate, source)
                sec = self.get_sleep_interval()
                print >> sys.stderr, "sleep " , sec , "s"
                time.sleep(sec)
        return 0
    def get_page_content(self, sec_url, first_id, sec_id, first_class, sec_class, cate, source):
        print >>sys.stderr, sec_url
        tgt_arts = []
        req = urllib2.Request(sec_url,headers=self.req_conf['headers'])
        rep = urllib2.urlopen(req)
        ul_tags = BeautifulSoup(rep.read().decode('utf-8')).find_all('ul', { "class" : "subcate-nav " + source.lower()})[0]
        sel_ul = ul_tags.nextSibling.nextSibling
        p_name_tags = sel_ul.find_all('p', {'class':'name'})
        for name in p_name_tags:
            if name.parent.attrs[u'class'][0] != 'app-meta':
                continue
            app_name = name.get_text()
            new_tag = name.nextSibling.nextSibling
            new_tag = new_tag.nextSibling.nextSibling
            if new_tag.attrs[u'class'][0] == 'down-size':
                [down_tag, size_tag] = new_tag.find_all('span')
                down = down_tag.get_text()
                size = size_tag.get_text()
            ret_info = [cate, first_class, sec_class, app_name, down, size]
            print "\t".join(ret_info)
        return 0

    def get_classes(self, tgt_arts, source):
        for tgt_art in tgt_arts:
            for tgt_dict in tgt_art:
                first_id = tgt_dict['first_id']
                sec_id = tgt_dict['sec_id']
                first_class = tgt_dict['first_class']
                sec_class = tgt_dict['sec_class']
                sec_url = tgt_dict['sec_url']
                cate = tgt_dict['source']
                ret = self.get_all_classes(sec_url, first_id, sec_id, first_class, sec_class, cate, source)
                times = 1
                while ret != 0 and times <= 3:
                    print >> sys.stderr, "get url:" + sec_url + " failed,times" + str(times)
                    ret = self.get_all_classes(sec_url, first_id, sec_id, first_class, sec_class, cate)
                    if ret == 0:
                        break
                    times += 1
                if times > 3:
                    print >> sys.stderr, "failed too many times,crawling process paused"
                    return 1
        print >> sys.stderr, "all classes have been got successfully!"
        return 0

    def get_articles_from_html(self,host,tgt_html,source):
        print >> sys.stderr, tgt_html
        tgt_arts = []
        req = urllib2.Request(tgt_html,headers=self.req_conf['headers'])
        rep = urllib2.urlopen(req)
        ul_tags = BeautifulSoup(rep.read().decode('utf-8')).find_all('ul', { "class" : "cate" })[0]
        sub_uls =  ul_tags.find_all('li', recursive = False)
        for sub in sub_uls:
            tgt_art = self.extract_information_from_tag(host,sub,source)
            if len(tgt_art) > 0:
                tgt_arts.append(tgt_art)
        ret = self.get_classes(tgt_arts, source)
        
        if ret == 1:
            print  >> sys.stderr, "get all info failed"
            return 1
        return 0
    #获取招聘的具体信息，标记感兴趣的信息和过滤掉不感兴趣的招聘信息
    def extract_information_from_tag(self,host,tgt_tag,source):
        tgt_art = {}
        tgt_art_list = []
        url_tag = tgt_tag.find_all('a',self.req_conf[source]['url_attr']) 
        tgt_art['source'] = source
        tgt_art['first_class'] = url_tag[0].get_text()
        tgt_art['url'] = host + url_tag[0]['href']
        tgt_art['first_id'] = re.search('\d+$',tgt_art['url']).group(0)
        sec_class_tag = tgt_tag.find_all('ul')[0]
        if sec_class_tag is None:
            print >> sys.stderr, "no sec class find"
            sys.exit(1)
        sec_classes = sec_class_tag.find_all('li')
        for sec_class in sec_classes:
            sec_url_tag = sec_class.find_all('a',self.req_conf[source]['sec_url_attr']) 
            tgt_art['sec_class'] = sec_url_tag[0].get_text()
            tgt_art['sec_url'] = host +  sec_url_tag[0]['href']
            tgt_art['sec_id'] = re.search('boardid(.+)$',tgt_art['sec_url']).group(0)
            tgt_art_list.append(copy.deepcopy(tgt_art))
        return tgt_art_list

    #执行爬取工作，程序刚启动时会爬取前两个页面的信息
    def run(self):
        print >> sys.stderr, "first time running"
        print >> sys.stderr, "time:" , time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        for source in self.job_sources:
            ret = self.get_articles_from_html(self.req_conf[source]['host'],self.req_conf[source]['url'],source)
            if ret == 1:
                os.system('php error.php "'+ source.lower() + ' spider failed"')
        self.first_flag = False
        print >> sys.stderr, "end time:" , time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        
'''
'''
reload(sys) 
sys.setdefaultencoding( "utf-8" )

if __name__ == '__main__':
    crawler_job = crawler(SOURCES)
    crawler_job.run()
    
