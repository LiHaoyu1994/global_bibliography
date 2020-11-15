"""
@encoding:utf-8
@author:Tommy
@time:2020/8/18　7:06
@note:The main function of the code is to provide common operation of all crawlers,
        including click,wait,jump and other opertions.
@备注:本段代码实现的主要功能为提供所有爬虫的常用操作,包括点击，等待，跳转等操作．
"""
import os
from threading import Thread, Lock
import pandas as pd
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException
from time import sleep
from lxml import etree
from crawlers.pymarc_tools import output_iso_from_data
import sys
import global_vars as gl

DRIVERS = []
DRIVERS_PAGES_TIMES = []
HOME_PAGE = {"Amazon": "https://www.amazon.com/",
             "Disco": "https://www.discogs.com/",
             "Neitherlands": "https://www.bol.com/nl/s/?",
             "Worldcat": "https://www.worldcat.org/",
             "Google": "https://www.google.com/",
             "South Korea": "https://www.nl.go.kr/",
             "Berkeley": "http://oskicat.berkeley.edu/search",
             "Yale": "https://orbis.library.yale.edu/vwebv/search?searchCode",
             "British": "http://search.bl.uk/primo_library/libweb/action/search.do?",
             "Michigan": "https://search.lib.umich.edu/catalog?library=All+libraries",
             "US": "https://catalog.loc.gov/"}

DRIVER_PATH = r'chromedriver.exe'

REFRESH_WAIT_TIME = 60
TOTAL_WAIT_TIME = 20
SINGLE_WAIT_TIME = 10
SHORT_WAIT_TIME = 1.5
isbn_total, data_total = [], {}
data_finding, data_found, data_not_found = set(), set(), set()
gLock = Lock()
CONTROL_CHARACTERS = ["\u200e"]

"""
@模块1:当输入一个数字时,启动对应数目的线程队列.
@输入参数: num:开启线程数 / pos_x:新建窗口横坐标位置 / pos_y:新建窗口纵坐标位置 / size_x:新建窗口宽度 
            / size_y: 新建窗口高度 
@返回参数: 生成的浏览器线程队列(list)
"""


def crawlers_queue(num: int, pos_x: float, pos_y: float, size_x: float, size_y: float):
    global DRIVERS, DRIVERS_PAGES_TIMES, gLock
    """
    @子方法1:调用该方法以启动一个新的子线程.
    """

    def set_driver():
        driver = webdriver.Chrome(executable_path=DRIVER_PATH)
        driver.set_page_load_timeout(TOTAL_WAIT_TIME)
        # 设置浏览器位置和大小 Set the browser location and size.
        driver.set_window_position(pos_x, pos_y)
        driver.set_window_size(size_x, size_y)
        # 生成新的线程,加入队列 Create a new thread, queue it
        gLock.acquire()
        DRIVERS.append(driver)
        DRIVERS_PAGES_TIMES.append(0)
        gLock.release()

    # 先更新DRIVERS_PAGES_TIMES的次数
    for i in range(len(DRIVERS_PAGES_TIMES)):
        DRIVERS_PAGES_TIMES[i] = 0
    thread_list = []
    for target_index in range(num):
        t = Thread(target=set_driver, args=())
        thread_list.append(t)
        t.start()
    for thread in thread_list:
        thread.join()


"""
@模块2:承接模块1输出的线程队列后,上锁并分配带搜索信息.循环调用并搜索,直到全部搜空为止.
@输入参数: num:开启线程数 / mission_name:欲爬取的网站简称 / output_file_name:输出文件位置
"""


def my_target(num: int, mission_name: str, output_file_name: str):
    global data_finding, data_found, DRIVERS, gLock

    # 各子线程执行的任务
    def thread_target(target_index: int):
        while True:
            # 上锁并判断是否有任务残留.如有,分配一条新信息.
            gLock.acquire()
            choices = set(isbn_total) - (data_finding | data_found)
            # 尚有目标待搜索
            if len(choices) > 0:
                isbn = list(choices)[0]
                data_finding.add(isbn)
                gLock.release()
                # 使用对应不同网站的具体操作检索对应的isbn编号
                get_detail_url(target_index, mission_name, isbn, output_file_name)
            # 任务已完成.停止分配并退出浏览器.
            else:
                gLock.release()
                break

    # 为各子线程开启任务,并加入任务栏
    thread_list = []
    for i in range(num):
        t = Thread(target=thread_target, args=(i,))
        thread_list.append(t)
        t.start()
    for thread in thread_list:
        thread.join()

    # 全部运行完毕后,删除全部的线程
    while len(DRIVERS) > 0:
        DRIVERS[0].quit()
        del DRIVERS[0]


"""
@模块3:根据输入不同参数,采用不同方式执行不同网站的爬虫操作.
"""


def get_detail_url(target_index: int, mission_name: str, isbn: str, output_file_name: str):
    global DRIVERS_PAGES_TIMES, data_finding, data_found, data_not_found, data_total, gLock, isbn_total
    # 修改该浏览器执行流程次数
    gLock.acquire()
    DRIVERS_PAGES_TIMES[target_index] += 1
    print("当前检测中线程:{} 检测中编码:{} 剩余进度:{}/{} continue数值:{}".format(target_index, isbn,
                                                                len(set(isbn_total) - (data_finding | data_found)),
                                                                len(isbn_total), data_total[isbn]['continue']))
    gLock.release()
    # 每次重启任务,需要从头开始首页面
    DRIVERS[target_index].execute_script("window.open('{}')".format(HOME_PAGE[mission_name]))
    DRIVERS[target_index].close()
    DRIVERS[target_index].switch_to.window(DRIVERS[target_index].window_handles[0])

    # 每个号码只给三次检测机会，三次机会结束后不再检测
    data = data_total[isbn]

    # 到达三次的时候,对0做一定的处理,直接输出结果并开始下一个检测
    if mission_name in ['Worldcat', 'Neitherlands', 'Disco', 'Amazon']:
        gLock.acquire()
        data_total[isbn]['continue'] += 1
        data = data_total[isbn]
        if data['continue'] == 4:
            data_finding.remove(isbn)
            data_found.add(isbn[2:])
            data_not_found.add(isbn[2:])
            isbn_total[isbn_total.index(isbn)] = isbn[2:]
            data_total[isbn[2:]] = data_total[isbn]
            data_total[isbn[2:]]['024a'] = data_total[isbn[2:]]['024a'][2:]
            del data_total[isbn]
            output_file(output_file_name, mission_name)
            gLock.release()
            return None
        gLock.release()
    """这里实现各网站具体爬取部分."""
    try:
        if mission_name == 'Amazon':
            # 如果是第一次进入页面
            if DRIVERS_PAGES_TIMES[target_index] == 1:
                # 单击箭头以弹出语种选择界面
                browser_click(target_index, "//a[@id='icp-nav-flyout']", True, True)
                # 在弹出的界面中,选择"English - EN" 即英语
                browser_click(target_index,
                              "//div[@class='a-row a-spacing-mini'][1]/div[@class='a-radio a-radio-fancy']"
                              "/label/i[@class='a-icon a-icon-radio']", True, True)
                # 选择后,点击"Save Changes" 保存更改
                browser_click(target_index, "//input[@class='a-button-input']", False, True)
                # 单击左上角配送地址,以将地址修改到英国
                browser_click(target_index, "//span[@id='glow-ingress-line2']", True, True)
                # 单击地区选择的选项卡
                browser_click(target_index, "//span[@id='GLUXCountryListDropdown']/span[@class='a-button-inner']"
                                            "/span[@class='a-button-text a-declarative']", True, True)
                # 获取所有选项
                choices = browser_find_word(target_index, "//ul[@class='a-nostyle a-list-link']/li/a", True, False)
                for index, choice in enumerate(choices):
                    if "United Kingdom" in choice:
                        # 点击英国按钮
                        browser_click(target_index, "//ul[@class='a-nostyle a-list-link']"
                                                    "/li[@class='a-dropdown-item'][{}]/a".format(index + 1), False,
                                      True)
                        # 点击保存按钮
                        browser_click(target_index, "//span[@class='a-button a-button-primary']"
                                                    "/span[@class='a-button-inner']/button[@class='a-button-text']",
                                      False, True)
                        sleep(2)
                        break

            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='twotabsearchtextbox']", isbn, True, True)
            # 搜索是否存在书目
            book_exist = browser_find_book(target_index, "//div[@id='a-page']/div[@id='search']",
                                           "//div[@class='a-section a-spacing-small a-spacing-top-small']")
            # 如果不存在书目,观察数据中的信息的continue字段+1后是否为3,如果是的话丢到data_not_found与data_found中
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                print("× 线程:{} 编号:{} 次数:{}".format(target_index, isbn, data_single['continue']))
                # 当continue没有到3次时,需要在在isbn_total对应的元素加0,同时也要在data_total对应的key调整前方加0
                isbn_total[isbn_total.index(isbn)] = "0" + isbn
                data_total["0" + isbn] = data_total[isbn]
                data_total["0" + isbn]['024a'] = '0' + data_total["0" + isbn]['024a']
                del data_total[isbn]
                data_finding.remove(isbn)
                gLock.release()
            else:
                # 存在书目
                data['备注'] = ''
                # 先观察题目中是否包含LP字样(例:028948156474)
                #   如果有'LP'字样,取出'[]'/'()'中的内容中数字部分,以'[LP {}]'形式写入'024c'字段
                title = browser_find_word(target_index, "//span[@class='a-size-medium a-color-base a-text-normal']",
                                          False, True)
                if 'LP' in title:
                    data['024c'] = '[LP 1]'
                # 获取题目下方蓝色粗体字样如"CD de audio"字样
                text = browser_find_word(target_index, "//div[@class='a-section a-spacing-none a-spacing-top-small']/"
                                                       "div[@class='a-row a-size-base a-color-base'][1]/"
                                                       "a[@class='a-size-base a-link-normal a-text-bold']", False,
                                         False)
                # 如果蓝色粗体带dvd字样(例:089948427292),则在'024c'字段标记
                if "DVD" in text:
                    data['024c'] = '[DVD 1]'
                # 单击该蓝色粗体字样以进入二级界面
                browser_click(target_index, "//div[@class='a-section a-spacing-none a-spacing-top-small']/"
                                            "div[@class='a-row a-size-base a-color-base'][1]/"
                                            "a[@class='a-size-base a-link-normal a-text-bold']", False, True)
                dict_ = browser_find_dict(target_index, "//a[@id='a-autoid-{}-announce']/span", 10, True)
                # '245a'字段: 题名
                #   例1:4010276019367(主要)
                data['245a'] = string_format(browser_find_word_after_loading(
                    target_index, "//h1[@id='title']/span[@id='productTitle']")).strip()
                #   例2:4010276026198
                if len(data['245a']) == 0:
                    data['245a'] = string_format(browser_find_word_after_loading(
                        target_index, "//div[@id='dmusicProductTitle_feature_div']/h1")).strip()
                # '245c' '100a'字段:爬取题名下方的蓝字部分,并根据有无逗号填充.有逗号为100a,否则为245c(例:4010276026198 Audio CD)
                words = browser_find_word_after_loading(target_index, "//div[@id='bylineInfo']/span/a").strip()
                if len(words) == 0:  # (例:4010276026198 Streaming)
                    words = browser_find_word_after_loading(target_index,
                                                            "//div[@id='ArtistLinkSection']/a").strip()
                if len(words) > 0:
                    if ',' in words:
                        data['100a'] = words
                    else:
                        data['245c'] = words
                    data.update(people_format(words))

                # 寻找按钮中,点击"Streaming Unlimited">"Audio CD"的优先级以获取字段(节目单例:4009850003403)
                for (k, v) in dict_.items():
                    if "CD de audio" in k:
                        browser_click(target_index, "//a[@id='a-autoid-{}-announce']/span".format(v), False, True)
                        break
                # 开始解析页面
                # 以zip方式去匹配内容界面,获取题目与内容
                title_content = \
                    browser_find_zip(target_index, "//div[@id='detailBullets_feature_div']/ul/li[{}]",
                                     ["/span/span[1]"], True, ele_garbages=["/style"])[0]
                # '024c'字段:用"Number of Discs"后的数字,输出[CD {}]/[LP {}]/[DVD {}]形式(例:0724357400626)
                data['024c'] = "[CD 1]"
                for (t, c) in title_content.items():
                    if "Number of Discs" in t:
                        if "LP" in data['024c']:
                            data['024c'] = "[LP {}]".format(c)
                        elif "DVD" in data['024c']:
                            data['024c'] = "[DVD {}]".format(c)
                        else:
                            data['024c'] = "[CD {}]".format(c)
                # '300a'字段:用"Number of Discs"后的数字,输出"{} audio disc(s)"形式(例:0724357400626)
                data['300a'] = "1 audio disc"
                for (t, c) in title_content.items():
                    if "Number of Discs" in t:
                        data['300a'] = "{} audio disc".format(c)
                        if c != '1':
                            data['300a'] += "s"
                # '028b'字段:用"Label"后的信息(例:0724357400626)
                for (t, c) in title_content.items():
                    if "Label" in t:
                        data['028b'] = c
                # '260b'字段:用"Label"后的信息
                for (t, c) in title_content.items():
                    if "Label" in t:
                        data['260b'] = c.strip()
                # '260c'字段:用"Date First Available"后的信息(例:0724357400626),将后面4位数字输出
                for (t, c) in title_content.items():
                    if "Date First Available" in t:
                        data['260c'], continue_num = '', 0
                        for index, word in enumerate(c):
                            if '0' <= word <= '9':
                                continue_num += 1
                            else:
                                continue_num = 0
                            if continue_num == 4:
                                data['260c'] = c[index - 3:index + 1]
                # '300b'/'300c' 字段固定分别为'digital', '4 3/4 in.'
                data['300b'], data['300c'] = 'digital', '4 3/4 in.'
                # '306a'字段:用"Total Length"后的信息进行六位数字填充 ; 同时在300a后面更新带括号的分钟秒钟信息
                for (t, c) in title_content.items():
                    if "Total Length" in t:
                        data['306a'] = time_format(c)[0].strip()
                        if "300a" in data:
                            data['300a'] += " " + time_format(c)[1].strip()
                        else:
                            data['300a'] = time_format(c)[1].strip()
                # '650'字段:用"Genres"后面部分填充
                for (t, c) in title_content.items():
                    if "Genres" in t:
                        data['650a'] = c.strip()
                # 寻找按钮中,点击"Streaming Unlimited"的按钮以获取节目单(节目单例:4009850003403)
                for (k, v) in dict_.items():
                    if "Streaming Unlimited" in k:
                        browser_click(target_index, "//a[@id='a-autoid-{}-announce']/span".format(v), False, True)
                        break
                # '505'字段:用节目单内容填充
                #       注: 当有多盘CD时:
                #           1.新的CD用'CD X.'开头2.节目之间用' ; '隔开 3.CD之间用' -- '隔开
                #           当只有一盘CD时:
                #           1.节目之间用' -- '隔开
                # todo 如果是多光盘的部分,如何调整节目单,待处理.
                try:
                    programme, has_chapter = \
                        browser_find_zip(target_index,
                                         ele_parent="//table[@id='dmusic_tracklist_content']/tbody/tr[{}]",
                                         ele_titles=["/td[1]"], long_wait=False, ele_garbages=None)
                    if len(programme) > 0:
                        data['505a'] = programme_format(programme, has_chapter)
                    if '300a' in data and int(data['300a'].split(" ")[0]) > 1 and not has_chapter:
                        data['备注'] += "多盘CD待分配"
                except TimeoutException:  # 没有找到节目单,超时以捕获
                    pass
                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_file(output_file_name, mission_name)
                gLock.release()
            """这里实现迪斯科部分的爬取."""
        elif mission_name == 'Disco':
            """这里实现迪斯科部分的爬取."""
            # 如果是第一次进入页面
            if DRIVERS_PAGES_TIMES[target_index] < 3:
                # 等待点击协议的"I Accept" 页面
                browser_click(target_index, "//button[@id='onetrust-accept-btn-handler']", True, False)
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='search_q']", isbn, True, True)
            # 搜索是否存在书目
            book_exist = browser_find_book(target_index, "//div[@id='page_content']/h1[@class='explore']",
                                           "//div[@id='search_results']")
            # 如果不存在书目,观察数据中的信息的continue字段+1后是否为3,如果是的话丢到data_not_found与data_found中
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                print("× 线程:{} 编号:{} 次数:{}".format(target_index, isbn, data_single['continue']))
                # 当continue没有到3次时,需要在在isbn_total对应的元素加0,同时也要在data_total对应的key调整前方加0
                isbn_total[isbn_total.index(isbn)] = "0" + isbn
                data_total["0" + isbn] = data_total[isbn]
                data_total["0" + isbn]['024a'] = '0' + data_total["0" + isbn]['024a']
                del data_total[isbn]
                data_finding.remove(isbn)
                gLock.release()
            else:
                # 存在书目
                data['备注'] = ''
                # 单击该图片以进入二级界面
                browser_click(target_index, "//span[@class='thumbnail_center']/img[1]", False, False)
                # (有的图片没有图标,例:4014513033543)
                browser_click(target_index, "//a[@class='default-image-as-icon thumbnail_link large-icon-size']"
                              , False, False)
                # 开始解析页面
                # 以分别抓取的方式重匹配内容界面,获取题目与内容
                title_content = browser_find_zip2(
                    target_index, "//div[@id='page_content']/div[@class='body']/div[@class='profile']",
                    ["/div[@class='head'][{}]"], True, ["/h1[@id='profile_title']"])
                # 增添下方'Companies,etc.'中的内容.
                title_content.update(
                    browser_find_zip(target_index, "//div[@id='companies']/div[@class='section_content toggle_"
                                                   "section_content']/ul[@class='list_no_style']/li[{}]",
                                     ["/span[@class='type']"], False)[0])
                # 增添下方'Barcode and Other Identifiers'中的内容
                title_content_temps = browser_find_words(
                    target_index, "//div[@id='barcodes']/div[@class='section_content toggle_section_content']/"
                                  "ul[@class='list_no_style']/li", False, symbol='\n')
                if len(title_content_temps) > 0:
                    for temp in title_content_temps.split("\n"):
                        key = temp.split(":")[0].strip()
                        title_content[key] = temp.split(":")[1].strip()
                # '245c'字段:获取题名前的'-'前部分,同时颠倒录入100a,700a
                data['245c'] = browser_find_words(target_index, "//h1[@id='profile_title']", False).split(
                    "‎–")[0].strip()
                # '245a' 保留题名的'-'后面部分.如后面再有'-'则拆分抛给245b.
                data['245a'] = browser_find_words(target_index, "//h1[@id='profile_title']", False).split(
                    "‎–")[1].strip()
                if '-' in data['245a']:
                    data['245b'] = data['245a'].split("-")[-1].strip()
                    data['245a'] = data['245a'][:data['245a'].rfind("-")].strip()
                # '100a' '700a'字段:利用245c更新.但是要先把字段中间都用分号切开.特别长的备注.
                words = []
                for word in data['245c'].split(" "):
                    if len(word.replace(",", "")) >= 3:
                        words.append(word)
                if len(words) > 2:
                    data['备注'] = '[245c]字段过长,请研究并手动填充[100a]与[700a]字段.'
                else:
                    str_ = words[0]
                    if len(words) == 2:
                        str_ += ", " + words[1]
                    data.update(people_format(str_))
                # '028a' '028b'字段
                #   Label:后面第一个" ‎– "之前的文字作为028b, " ‎– "后面的数字部分作为028a
                #   若有2组028的信息， 中间使用逗号分开的，逗号后边文字部分作为028b, 数字部分作为028a
                for (t, c) in title_content.items():
                    if t == "Label":
                        words = c.strip()
                        symbols, symbol = ["‎–", "–"], ""
                        for sym in symbols:
                            if sym in words:
                                symbol = sym
                        if "," in words:
                            data['028b'] = words.split(",")[-1].strip()
                            data['028a'] = data['028b'].split(symbol)[0].strip()
                        else:
                            data['028a'] = words.split(symbol)[1].strip()
                            data['028b'] = words.split(symbol)[0].strip()
                # '024c'字段:从Format后面最前面如果有数字就写入024c
                cd_num = ""
                for (t, c) in title_content.items():
                    if t == "Format":
                        for letter in c[:6].strip():
                            if '0' <= letter <= '9':
                                cd_num += letter
                if len(cd_num) >= 1:
                    data['024c'] = "[CD {}]".format(cd_num)
                else:
                    data['024c'] = "[CD 1]"
                # '260a'字段:从Country抓取
                for (t, c) in title_content.items():
                    if "Country" in t:
                        data['260a'] = c.strip()
                # '260b'字段:从Copyright (c) 和Manufactured By中抓取.如果都存在,中间用' : '连接
                for (t, c) in title_content.items():
                    if "Copyright (c)" in t:
                        if '260b' not in data:
                            data['260b'] = c.replace("–", "").strip()
                        else:
                            data['260b'] += ' : ' + c.replace("–", "").strip()
                    elif "Manufactured By" in t:
                        if '260b' not in data:
                            data['260b'] = c.replace("–", "").strip()
                        else:
                            data['260b'] += ' : ' + c.replace("–", "").strip()
                # '260c'字段:Released后面数字部分
                for (t, c) in title_content.items():
                    if t == "Released":
                        data['260c'] = ""
                        for word in c:
                            if '0' <= word <= "9":
                                data['260c'] += word
                # '650a'字段:Style后面部分
                for (t, c) in title_content.items():
                    if t == "Style":
                        data['650a'] = c.strip()
                # '024a'字段:Barcode后面部分
                for (t, c) in title_content.items():
                    if t == "Barcode":
                        data['024a'] = c.strip().replace(" ", "")
                # '511a'字段:Credit单元块内蓝色字体
                # todo 这里的灰色字体尚未出现,如若出现需要写在ele_garbages中.
                title_content2 = browser_find_zip(
                    target_index, "//div[@id='credits']/div[@class='section_content toggle_section_content']/"
                                  "ul[@class='list_no_style']/li[{}]", ["/span[@class='role']"], False)[0]
                for (k, v) in title_content2.items():
                    if '511a' not in data:
                        data['511a'] = ""
                    else:
                        data['511a'] += ' ; '
                    data['511a'] += "{}{}, {}".format(v[:2].replace("–", "").strip(), v[2:].strip(), k.lower())
                # '505a'字段:抓取节目单(有的有序列号)
                data['505a'] = browser_find_zip3(target_index, "//table[@class='playlist']/tbody/tr[{}]", False).strip()

                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_file(output_file_name, mission_name)
                gLock.release()
        elif mission_name == "Neitherlands":
            """这里实现荷兰网站的爬取."""
            if DRIVERS_PAGES_TIMES[target_index] < 3:
                # 等待点击协议的"Accepteren" 页面
                browser_click(target_index, "//button[@class='js-confirm-button']/span", False, False)
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='searchfor']", isbn, True, False)
            if DRIVERS_PAGES_TIMES[target_index] < 3:
                # 有可能再次弹出"Accepteren" 页面
                browser_click(target_index, "//button[@class='js-confirm-button']/span", False, False)
                sleep(1)
            # 捕获上方的黑色字体,判断isbn编号是否在其中以判断是否有书目
            #       注:这里ele_none实在找不到  随便写了一个,不影响运行.
            book_exist = browser_find_book(
                target_index, "//main[@id='mainContent']", "//div[@class='h-o-hidden']/a[@class='product-image produ"
                                                           "ct-image--list px_list_page_product_click']/img")
            # 如果不存在书目,观察数据中的信息的continue字段+1后是否为3,如果是的话丢到data_not_found与data_found中
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                print("× 线程:{} 编号:{} 次数:{}".format(target_index, isbn, data_single['continue']))
                # 当continue没有到3次时,需要在在isbn_total对应的元素加0,同时也要在data_total对应的key调整前方加0
                isbn_total[isbn_total.index(isbn)] = "0" + isbn
                data_total["0" + isbn] = data_total[isbn]
                data_total["0" + isbn]['024a'] = '0' + data_total["0" + isbn]['024a']
                del data_total[isbn]
                data_finding.remove(isbn)
                gLock.release()
            else:
                # 存在书目
                data['备注'] = ''
                # 单击该图片以进入二级界面
                browser_click(target_index, "//a[@class='product-image product-image--list px_list_page_"
                                            "product_click']/img[1]", False, False)
                # 单击第一个显示更多的按钮"Toon meer"(例:4011222317254)
                browser_click(target_index, "//button[@class='show-more__button']", False, False)
                sleep(1.5)
                # 单击第二个显示更多的按钮"Toon meer"(例:4011222317254)
                browser_click(target_index, "//a[@class='show-more__button']", False, False)
                sleep(1.5)
                # 开始解析页面
                # 以分别抓取的方式重匹配内容界面,获取题目与内容
                # 抓取上面的模块
                title_content = browser_find_zip4(target_index, "//div[@class='specs__party-group'][{}]", True)
                # 抓取下面的序列模块
                title_content.update(
                    browser_find_zip5(target_index, "//div[@class='specs'][{}]", "/dl/dt[{}]", "/dl/dd[{}]"))
                # '024a' '028a'字段.
                #       '024a'字段由'EAN'内容生成.
                #       '028a'字段由'EAN'内容倒数七位之前生成.
                for (t, c) in title_content.items():
                    if t == "EAN":
                        data['024a'] = c.strip().replace(" ", "")
                        data['028a'] = c.strip()[:-7]
                # '024c'字段.
                #       '024c'字段由'Aantal stuks in verpakking'内容生成.
                for (t, c) in title_content.items():
                    if t == "Aantal stuks in verpakking":
                        data['024c'] = "[CD {}]".format(c.split("disk")[0].strip())
                # '245a'字段.
                #       '245a'字段由题名":"后面的部分组成.句子首字母大写,其它小写.
                words = browser_find_word(target_index, "//h1[@class='page-heading']/span", False, False
                                          ).split(":")[-1].strip()
                data['245a'] = words[0].upper() + words[1:].lower()
                # '245c'字段
                #       todo 多个责任者待做
                #       优先级"Componist(en)">"Artiest(en)" 如果有Various则略去
                for (t, c) in title_content.items():
                    if t == "Componist(en)":
                        data['245c'] = c.strip()
                        break
                    elif t == "Artiest(en)" and "Various" not in c.strip():
                        data['245c'] = c.strip()
                # '100a'字段
                #       '100a'字段由"Componist(en)"后面的部分颠倒后大小写构成.
                for (t, c) in title_content.items():
                    if t == "Componist(en)" and "Various" not in c.strip():
                        data.update(people_format(c.strip()))
                # '041a'字段
                #       '041a'字段由"Taal"内容前三位小写构成
                for (t, c) in title_content.items():
                    if t == "Taal":
                        data['041a'] = c.strip()[:3].lower()
                # '260b'字段
                #       '260b'字段由"Label"内容构成
                for (t, c) in title_content.items():
                    if t == "Label":
                        data['260b'] = c.strip()
                # '260c'字段
                #       '260c'字段由"Releasedatum"内容后面四位数字构成
                for (t, c) in title_content.items():
                    if t == "Releasedatum":
                        data['260c'] = ""
                        for i in range(len(c.strip())):
                            if '260c' in data and len(data['260c']) == 4:
                                break
                            if '0' <= c.strip()[-i - 1] <= '9':
                                data['260c'] = c.strip()[-i - 1] + data['260c']
                # '300a'字段
                #       '300a'字段由'024c'中的数字+"Speelduur"内容生成
                for (t, c) in title_content.items():
                    if t == "Speelduur":
                        cd_num = "1"
                        if '024c' in data:
                            cd_num = data['024c'].split("CD")[1].split("]")[0].strip()
                        if cd_num == "1":
                            data['300a'] = "{} audio disc ({})".format(cd_num, c.strip())
                        else:
                            data['300a'] = "{} audio discs ({})".format(cd_num, c.strip())
                # '650a'字段
                #       '650a'字段由"Muziekgenre"内容构成
                for (t, c) in title_content.items():
                    if t == "Muziekgenre":
                        data['650a'] = c.strip()
                # '505a'字段
                page_source = etree.HTML(DRIVERS[target_index].page_source)
                words_num = len(page_source.xpath("//div[@class='tracklist']/ul/li/span[3]/text()"))
                try:
                    if words_num > 0:
                        data['505a'] = ""
                        for i in range(words_num):
                            if len(data['505a']) > 0:
                                data['505a'] += ' -- '
                            data['505a'] += "{}.".format(i + 1) + page_source.xpath(
                                "//div[@class='tracklist']/ul/li[{}]/span[3]/text()".format(i + 1))[0].strip()
                #       '505'的特殊情况,多个Tracklist(例:8025726119296)
                except IndexError:
                    try:
                        data['505a'] = ""
                        track_num = len(page_source.xpath("//div[@class='tracklist']"))
                        for i in range(track_num):
                            if len(data['505a']) > 0:
                                data['505a'] += ' -- '
                            data['505a'] += 'CD {}:'.format(i + 1)
                            words_num = len(page_source.xpath("//div[@class='tracklist'][{}]/ul/li/"
                                                              "span[3]/text()".format(i + 1)))
                            for j in range(words_num):
                                if j > 0:
                                    data['505a'] += ' ; '
                                data['505a'] += "{}.".format(j + 1) + page_source.xpath(
                                    "//div[@class='tracklist'][{}]/ul/li[{}]/span[3]/text()".format(i + 1, j + 1))[
                                    0].strip()
                    #           '505'的特殊情况,例:7318590014141
                    except IndexError:
                        del data['505a']

                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_file(output_file_name, mission_name)
                gLock.release()
        elif mission_name == "Worldcat":
            #   尝试点击接受cookie按钮
            if DRIVERS_PAGES_TIMES[target_index] < 3:
                # 等待点击协议的"接受cookie" 页面
                browser_click(target_index, "//button[@id='onetrust-accept-btn-handler']", False, True)
            # 在"光盘"的输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='q1']", isbn, True, True)
            # 搜索是否存在书目
            book_exist = browser_find_book(target_index, "//div[@id='div-footermessage']",
                                           "//table[@id='br-table-results']/tbody/tr/td/a/img")
            # 如果不存在书目,观察数据中的信息的continue字段+1后是否为3,如果是的话丢到data_not_found与data_found中
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                print("× 线程:{} 编号:{} 次数:{}".format(target_index, isbn, data_single['continue']))
                # 当continue没有到3次时,需要在在isbn_total对应的元素加0,同时也要在data_total对应的key调整前方加0
                isbn_total[isbn_total.index(isbn)] = "0" + isbn
                data_total["0" + isbn] = data_total[isbn]
                data_total["0" + isbn]['024a'] = '0' + data_total["0" + isbn]['024a']
                del data_total[isbn]
                data_finding.remove(isbn)
                gLock.release()
            else:
                # 存在书目
                data['备注'] = ''
                # 单击该蓝色粗体字样以进入二级界面
                browser_click(target_index, "//table[@id='br-table-results']/tbody/tr[1]/td/div/a/strong", False, True)
                # 开始解析页面
                # 以zip方式去匹配内容上方界面,获取题目与内容
                title_content = browser_find_zip(target_index, "//div[@id='bibdata']/table/tbody/tr[{}]",
                                                 ["/th"], True)[0]
                title_content.update(browser_find_zip(target_index, "//div[@id='details']/div/table/tbody/tr[{}]",
                                                      ["/th"], True)[0])
                # '245a' '245b'字段:最上面的题名中,如有冒号,则按冒号分拨;否则全部算在245a下.
                words = browser_find_word(target_index, "//div[@id='bibdata']/h1", False, False).strip()
                data['245a'] = words.split(":")[0].strip()
                if ":" in words:
                    data['245b'] = words[words.find(":") + 1:].strip()
                # '245c'字段:上方著者信息
                for (k, v) in title_content.items():
                    if k.strip() == "著者：":
                        data['245c'] = v.strip()
                        data.update(people_format(data['245c']))
                # '260a' '260b' '260c'字段   上方出版商信息
                #       '260a':     ":"前
                #       '260b':     ":"与","之间  /  ":"到数字之间
                #       '260c':     后面的四位数字年份.
                for (k, v) in title_content.items():
                    if k.strip() == "出版商：":
                        words = v.strip()
                        data['260a'], words = words.split(":")[0].strip(), words[words.find(":") + 1:].strip()
                        data['260b'], data['260c'], add_260b = '', '', True
                        for word in words:
                            if '0' <= word <= '9' or word == ',':
                                add_260b = False
                            if add_260b:
                                data['260b'] += word
                            if '0' <= word <= '9':
                                data['260c'] += word
                # '650a'字段 上方主题信息.中间使用' ; '隔开.
                words = etree.HTML(DRIVERS[target_index].page_source).xpath("//ul[@id='subject-terms']/li/a/text()")
                if len(words) > 0:
                    data['650a'] = ""
                    for word in words:
                        if len(word.strip()) > 0:
                            if len(data['650a']) > 0 and data['650a'][-1] == '.':
                                data['650a'] = data['650a'][:-1]
                            if len(data['650a']) > 0:
                                data['650a'] += ' ; '
                            data['650a'] += word.strip()

                # '490a'字段 上方丛书信息
                for (k, v) in title_content.items():
                    if "丛书" in k:
                        data['490a'] = v.replace(". ", " ; ").strip()
                # '511a'字段:下方信息中的"演员"信息
                for (k, v) in title_content.items():
                    if k.strip() == "演员：":
                        data['511a'] = v.strip()
                # '300a' '300b' '300c'字段:下方信息中的"描述"信息
                #       '300a': ':'前的部分
                #       '300b': ':'-';'之间的部分
                #       '300c': ';'之后的部分
                for (k, v) in title_content.items():
                    if k.strip() == "描述：":
                        data['300a'] = v.strip()
                        if ":" in data['300a']:
                            data['300b'] = data['300a'][data['300a'].rfind(":") + 1:].strip()
                            data['300a'] = data['300a'][:data['300a'].rfind(":")].strip()
                        if '300b' in data and ";" in data['300b']:
                            data['300c'] = data['300b'][data['300b'].rfind(";") + 1:].strip()
                            data['300b'] = data['300b'][:data['300b'].rfind(":")].strip()
                # '505a'字段:下方信息中的"内容"信息
                for (k, v) in title_content.items():
                    if k.strip() == "内容：":
                        data['505a'] = v.strip()

                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_file(output_file_name, mission_name)
                gLock.release()
        elif mission_name == "Google":
            data['备注'] = ''
            # 有的数据没有找到245c 跳过
            if '245c' not in data:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            # 在"光盘"的输入框中输入245c
            #       如果245c内容有",",则在100a与245c结尾注明并退出
            elif '245c' in data and "," in data['245c']:
                # print("多作者需检查")
                data['245c'] += "多作者需检查"
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("×× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # print("isbn:{}  data['245c']:{}".format(isbn, data['245c']))
                browser_input_keyword(target_index, "//input[@class='gLFyf gsfi']", data['245c'], True, True)
                titles = browser_find_words2(target_index, "//div[@id='rso']/div[@class='g']/div/div/a/h3", True)
                # 寻找带有wikipedia的标题,点击
                for index, title in enumerate(titles):
                    if 'Wikipedia' in title:
                        browser_click(target_index, "//div[@id='rso']/div[@class='g'][{}]/div/div/a/h3".format(
                            index + 1), False, True)
                        words = browser_find_words2(target_index, "//div[@class='mw-parser-output']/p[1]", True)
                        if len(words) == 0:
                            words = browser_find_words2(target_index, "//div[@class='mw-parser-output']/p[2]", False)
                        # 100d:从words中找两个连续四个数字,作为生卒年填充100d
                        birth_year, continue_num = [], 0
                        for text in words:
                            continue_num = 0
                            for index2, word in enumerate(text):
                                if '0' <= word <= '9':
                                    continue_num += 1
                                else:
                                    continue_num = 0
                                if continue_num == 4:
                                    birth_year.append(text[index2 - 3:index2 + 1])
                        if len(birth_year) >= 2:
                            data['100d'] = "{}-{}".format(birth_year[0], birth_year[1])
                        # 100e:存在责任身份,如果字符中有"composer"或者"Composer" 则在100e填写"composer"
                        for text in words:
                            if "composer" in text or "Composer" in text:
                                data['100e'] = "composer"
                                break
                        break

                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_file(output_file_name, mission_name)
                gLock.release()
        elif mission_name == "South Korea":
            # 跳过输入isbn操作,直接通过网站访问
            DRIVERS[target_index].execute_script("window.open('https://www.nl.go.kr/NL/contents/search.do?"
                                                 "srchTarget=total&pageNum=1&pageSize=10&kwd={}')".format(isbn))
            DRIVERS[target_index].close()
            DRIVERS[target_index].switch_to.window(DRIVERS[target_index].window_handles[0])
            # 搜索是否存在书目
            book_exist = browser_find_book(target_index, "//div[@class='search_left_section']",
                                           "//a[@class='btn_layer detail_btn_layer']")
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # 点击题目
                browser_click(target_index, "//div[@class='row']/span[@class='txt_left row_txt_tit']/a", True, True)
                # 点击MARC按钮会弹出新的窗口(灰色按钮),因此获取MARC按钮的新链接
                new_url = browser_get_attribute(target_index, "//button[@class='btn dark_btn'][2]", "onclick", True,
                                                True)
                new_url = new_url.split(",")[0].replace("'", "").replace("javascript:window.open(", "").strip()
                # 弹出新的数据页
                DRIVERS[target_index].execute_script(
                    "window.open('{}/{}')".format(HOME_PAGE[mission_name][:-1], new_url[1:]))
                DRIVERS[target_index].close()
                DRIVERS[target_index].switch_to.window(DRIVERS[target_index].window_handles[0])
                # 开始抓取数据
                data = browser_find_zip(target_index, "//tbody/tr[{}]", ["/td[1]"], True)[0]
                #       替换掉题名
                data2 = {"isbn": isbn,
                         "head": browser_find_word(target_index, "//table[1]/tbody/tr/td[@class='left']", False, False)}
                # 排序, 同时把前面标题号部分多余摘取的删除.
                for (k, v) in data.items():
                    if k != '' and len(k) <= 7:
                        data2[k] = v
                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data2)
                data_found.add(isbn)
                # 把结果输出到csv文件中
                output_file(output_file_name, mission_name)
                gLock.release()
        elif mission_name == "Berkeley":
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='searcharg']", isbn, True, True)
            # 搜索是否存在书目(7318590014141无/9781945498640有1/9781119512264有2)
            book_exist = browser_find_book(target_index, "//div[@class='topLogoDiv']",
                                           "//div[@class='wblinkdisplay']/form/a/img")
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # 点击题目
                browser_click(target_index, "//span[@class='briefcitTitle']/a", True, False)
                # 点击MARC DISPLAY按钮
                WebDriverWait(DRIVERS[target_index], SINGLE_WAIT_TIME).until(
                    ec.presence_of_element_located((By.XPATH, "//div[@id='bibDisplayBody']"))
                )
                button_text, marc_display_index = {}, -1
                for i in range(10):
                    button_text[i] = etree.HTML(DRIVERS[target_index].page_source).xpath(
                        "//div[@id='bibDisplayBody']/div[1]/a[{}]/img/@title".format(i + 1))
                    if 'MARC Display' in button_text[i]:
                        marc_display_index = i
                        break
                browser_click(target_index,
                              "//div[@id='bibDisplayBody']/div[1]/a[{}]/img".format(marc_display_index + 1), False,
                              True)
                # 获取全部数据内容
                text = browser_find_word(target_index, "/html/body/div[@class='clear']/div[2]/pre", False, True)
                # 将数据内容转化为常用的data格式.
                data = Berkeley_convert_data(text)
                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_iso_from_data(output_file_name, isbn_total, data_total)
                gLock.release()
        elif mission_name == "Yale":
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='searchArg']", isbn, True, True)
            # 搜索是否存在书目(7318590014141无/9781945498640有1/9781119512264有2)
            book_exist = browser_find_book(target_index, "//div[@id='headerRow']",
                                           "//div[@class='thisItem']/ul/li[2]/a/span")
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # 点击右侧"Staff Marc Review"
                browser_click(target_index, "//div[@class='thisItem']/ul/li[2]/a/span", True, False)
                # 从Yale网站获取数据
                data = Yale_convert_data(target_index)

                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_iso_from_data(output_file_name, isbn_total, data_total)
                gLock.release()
        elif mission_name == "British":
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='search_field']", isbn, True, True)
            # 搜索是否存在书目(7318590014141无/9781945498640有1/9781119512264有2)
            book_exist = browser_find_book(target_index, "//div[@id='resultsNumbersTile']/h1",
                                           "//div[@class='EXLSummaryFields']")
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # 点击第一个标题
                browser_click(target_index, "//h2[@class='EXLResultTitle']/a", True, True)
                # 点击"Details"
                browser_click(target_index, "//li[@id='exlidResult0-DetailsTab']/a", True, True)
                # 获取"MARC display"背后的href,打开页面
                new_url = browser_get_attribute(target_index, "//div[@class='EXLDetailsLinks']/ul/li[last()]/span["
                                                              "@class='EXLDetailsLinksTitle']/a", "href", True, True)
                # 弹出新的数据页
                DRIVERS[target_index].execute_script("window.open('{}')".format(new_url))
                DRIVERS[target_index].close()
                DRIVERS[target_index].switch_to.window(DRIVERS[target_index].window_handles[0])
                # 整理数据
                data = British_convert_data(target_index)
                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_iso_from_data(output_file_name, isbn_total, data_total)
                gLock.release()
        elif mission_name == "Michigan":
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='search-query']", isbn, True, True)
            # 搜索是否存在书目.由于最开始提示字样为Loading,因此当识别为非Loading时跳出.
            book_exist = True
            while True:
                book_hint = etree.HTML(DRIVERS[target_index].page_source).xpath(
                    "//div[@class='results-summary-container']/h2[@class='results-summary']/text()")[0]
                book_hint = book_hint.replace('\t', '').replace('\n', '').strip()
                if "Loading" not in book_hint:
                    if "match" in book_hint:  # 有的提示为"match your search".所以应用"match"等字样匹配
                        book_exist = False
                    break
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # 点击第一个标题
                browser_click(target_index, "//h3[@class='record-title']/a/span/span", True, True)
                # 点击下方"View MARC data"按钮
                browser_click(target_index, "//button[@class='css-kfqkh3']", True, True)
                # 开始整理数据
                data = Michigan_convert_data(target_index)

                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_iso_from_data(output_file_name, isbn_total, data_total)
                gLock.release()
        elif mission_name == "US":
            # 第一次打开 等待重定向的五秒钟
            if DRIVERS_PAGES_TIMES[target_index] == 1:
                sleep(5)
            # 在输入框中输入isbn编号
            browser_input_keyword(target_index, "//input[@id='quick-search-argument']", isbn, True, True)
            # 搜索是否存在书目.
            book_exist = browser_find_book(target_index, "//main[@class='content-wrapper']/article",
                                           "//li[@class='tab']/a")
            if not book_exist:
                gLock.acquire()
                data_single = data_total[isbn]
                data_total[isbn] = data_single
                print("× 线程:{} 编号:{}".format(target_index, isbn))
                data_finding.remove(isbn)
                data_found.add(isbn)
                data_not_found.add(isbn)
                gLock.release()
            else:
                # 点击第一个标题
                browser_click(target_index, "//div[@class='search-results-list-description-item search-results-"
                                            "list-description-title']/a", True, False)
                # 点击下方"MARC Tags"按钮
                browser_click(target_index, "//li[@class='tab']/a", True, True)
                # 开始整理数据
                data = US_convert_data(target_index)
                # 上锁,并在data_finding中删除编号 在data_found中添加编号
                gLock.acquire()
                print("√ 线程:{} 编号:{} ".format(target_index, isbn))
                data_finding.remove(isbn)
                data_total[isbn].update(data)
                data_found.add(isbn)
                # 把结果输出到文件中
                output_iso_from_data(output_file_name, isbn_total, data_total)
                gLock.release()
    # 如果超时/点击太快被干扰,重新搜索
    except (TimeoutException, ElementClickInterceptedException):
        try:  # DISCO速度过快导致屏蔽,需等待.
            WebDriverWait(DRIVERS[target_index], SHORT_WAIT_TIME).until(
                ec.presence_of_element_located((By.XPATH, "//div[@id='wrapper']/div[@id='broken_record']"))
            )
            sleep(REFRESH_WAIT_TIME)
        except TimeoutException:
            pass
        gLock.acquire()
        data_finding.remove(isbn)
        gLock.release()
    except IOError:  # 文件已被打开,占用中
        print("文件打开中,请尽快关闭!")
    finally:
        pass


"""
模块4:从csv文件将关键词提成key,根据优先级(1)"024a"(2)"028a"生成新的字典.
@输入参数: file_name:待提取的文件名称
@输出参数: data_total:以关键字为key,其它信息为value的字典(dict)
            isbn_total:全部待搜索的isbn整体
@注: 在生成字典时,添加一个continue字段,以记录当前已添加'0'的次数.
"""


def get_isbn_from_csv(file_name: str) -> dict:
    global data_total, isbn_total
    data_total_temp = pd.read_csv(file_name, encoding='utf-8', dtype=str).to_dict(orient='records')
    # 先把表格中的全部信息录入data_total中.注意,如果是nan的部分,则删掉不计入
    try:
        for index, value in enumerate(data_total_temp):
            data_single = {}
            for k in value:
                v = str(value[k])
                if v == 'nan' or len(v.strip()) == 0:
                    pass
                else:
                    data_single[k] = v.strip()
            data_single['continue'] = 0
            if '024a' in data_single:
                isbn_total.append(data_single['024a'])
                data_total[data_single['024a']] = data_single
            else:
                isbn_total.append(data_single['028a'])
                data_total[data_single['028a']] = data_single
    except KeyError:
        print("读入表格失败.请观察源数据中是否有空行并排除.")
        sys.exit()
    return data_total


"""
@模块5:从txt读取isbn编号
"""


def get_isbn_from_txt(file_name: str) -> dict:
    global data_total, isbn_total
    # 先把文件中的全部信息录入data_total中.
    fp = open(file_name, 'r', encoding='utf-8')
    words = fp.readline()
    while len(words) > 0:
        if len(words.strip()) > 0:
            # 有的首行有\ufeff,需要清除
            isbn_total.append(words.replace("\ufeff", "").strip())
            data_total[words.replace("\ufeff", "").strip()] = {"continue": 0}
        words = fp.readline()
    return data_total


"""
@模块6:使用指定浏览器获取指定元素模块的属性值.
"""


def browser_get_attribute(target_index: int, ele: str, attribute_name: str, long_wait: bool,
                          exist_for_sure: bool) -> str:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    WebDriverWait(DRIVERS[target_index], wait_time).until(
        ec.presence_of_element_located((By.XPATH, ele))
    )
    if exist_for_sure:
        text = etree.HTML(DRIVERS[target_index].page_source).xpath(ele + "/@{}".format(attribute_name))[0]
        return text
    else:
        try:
            text = etree.HTML(DRIVERS[target_index].page_source).xpath(ele + "/@{}".format(attribute_name))[0]
            return text
        except NoSuchElementException:
            return ""


"""
@模块5:使用指定浏览器等待某元素模块从网页中出现,并输入信息
@输入参数: target_index:浏览器编号 / ele:模块的元素名 / info:输入的信息 / send_enter:输入后是否输入回车
            / long_wait:是否长等待
"""


def browser_input_keyword(target_index: int, ele: str, info: str, send_enter: bool, long_wait: bool):
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    WebDriverWait(DRIVERS[target_index], wait_time).until(
        ec.presence_of_element_located((By.XPATH, ele))
    )

    input_tag = DRIVERS[target_index].find_element_by_xpath(ele)
    input_tag.send_keys(info)
    if send_enter:
        input_tag.send_keys(Keys.ENTER)


"""
@模块6:使用指定浏览器等待某元素模块从网页中出现,并单击该元素
@输入参数: target_index:浏览器编号 / ele:模块的元素名 / long_wait:是否长等待/ exist_for_sure: 是否一定会出现,涉及判断网速
"""


def browser_click(target_index: int, ele: str, long_wait: bool, exist_for_sure: bool):
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    try:
        if not exist_for_sure:
            wait_time = SINGLE_WAIT_TIME
            try:
                WebDriverWait(DRIVERS[target_index], wait_time).until(
                    ec.presence_of_element_located((By.XPATH, ele))
                )
                button_tag = DRIVERS[target_index].find_element_by_xpath(ele)
                button_tag.click()
            except TimeoutException:
                pass
        else:
            WebDriverWait(DRIVERS[target_index], wait_time).until(
                ec.presence_of_element_located((By.XPATH, ele))
            )
            button_tag = DRIVERS[target_index].find_element_by_xpath(ele)
            button_tag.click()
    except Exception as e:
        print("browser_click中出现了报错.报错类型:{}.报错信息:{}".format(type(e), e))


"""
@模块7:使用指定浏览器根据具体书单的判别模块出现,以判别书单数目
@输入参数:target_index:浏览器编号 / ele_none:有没有书目都存在的xpath / ele_found:书目独有的xpath
@返回参数: 是否找到书目 bool 
"""


def browser_find_book(target_index: int, ele_none: str, ele_found: str) -> bool:
    global DRIVERS
    result = False
    WebDriverWait(DRIVERS[target_index], 3 * SHORT_WAIT_TIME).until(
        ec.presence_of_element_located((By.XPATH, ele_none))
    )
    try:
        DRIVERS[target_index].find_element_by_xpath(ele_found)
        result = True
    except NoSuchElementException:
        pass
    return result


"""
@模块8:使用指定浏览器获取某元素模块的字样内容.可能返回str,也可能返回list.
@输入参数: target_index:浏览器编号 / ele:模块的元素名 / is_list:是否返回一个list / long_wait:是否长等待
@注:这里自动实现了在尾缀添加"/text()"功能
"""


def browser_find_word(target_index: int, ele: str, is_list: bool, long_wait: bool):
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    WebDriverWait(DRIVERS[target_index], wait_time).until(
        ec.presence_of_element_located((By.XPATH, ele))
    )
    text = etree.HTML(DRIVERS[target_index].page_source).xpath(ele + "/text()")
    if not is_list:
        for word in text:
            if len(word.strip()) > 0:
                return word
        return None
    else:
        result = []
        for word in text:
            if len(word.strip()) > 0:
                result.append(word.strip())
        return result


"""
@模块9:使用指定浏览器获取某元素模块的全部字样合并后的内容.返回str
@输入参数: target_index:浏览器编号 / ele:模块的元素名 / long_wait:是否长等待
@注:这里自动实现了在尾缀添加"/text()"功能
"""


def browser_find_words(target_index: int, ele: str, long_wait: bool, symbol=" ") -> str:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    try:
        WebDriverWait(DRIVERS[target_index], wait_time).until(
            ec.presence_of_element_located((By.XPATH, ele))
        )
        text = etree.HTML(DRIVERS[target_index].page_source).xpath(ele + "//text()")
        result = ""
        for word in text:
            if len(word.strip()) > 0:
                if len(result.strip()) > 0:
                    result += symbol
                result += word.strip()
        return result
    except TimeoutException:
        return ""


"""
@模块10:使用浏览器获取某元素模块的全部字样的内容.返回list
@注:这里自动实现了在尾缀添加"//text()"功能
"""


def browser_find_words2(target_index: int, ele: str, long_wait: bool) -> list:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    try:
        WebDriverWait(DRIVERS[target_index], wait_time).until(
            ec.presence_of_element_located((By.XPATH, ele))
        )
        result = etree.HTML(DRIVERS[target_index].page_source).xpath(ele + "//text()")
        for index in range(len(result)):
            result[index] = result[index].strip()
        return result
    except TimeoutException:
        return []


"""
@模块10:使用指定浏览器获取某一系列模块中,数字与对应的字样内容的关系,以"内容-序号"的字典形式返回
@输入参数: target_index:浏览器编号 / ele:该系列模块的元素名 / max_index: 需要解析的数字最大值 / long_wait: 是否长等待
@注:本模块在页面内容方面不需要匹配
"""


def browser_find_dict(target_index: int, ele: str, max_index: int, long_wait: bool) -> dict:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    times = wait_time * 2
    while times > 0:
        times -= 1
        for i in range(max_index):
            text = etree.HTML(DRIVERS[target_index].page_source).xpath(ele.format(i) + "/text()")
            # print("循环次数:{} 当前检索下标:{} 含有抽屉数目:{}".format(wait_time * 2 - times, i, len(text)))
            if len(text) > 0:
                times = 0
                break
        sleep(0.5)

    result = {}
    for i in range(max_index):
        texts = etree.HTML(DRIVERS[target_index].page_source).xpath(ele.format(i) + "/text()")
        if len(texts) > 0:
            result[i] = ''
            for text in texts:
                if len(text.strip()) > 0:
                    if len(result[i]) > 0:
                        result[i] += ' '
                    result[i] += text.strip()
    result2 = {}
    for temp in result:
        result2[result[temp]] = temp

    return result2


"""
@模块11:使用指定浏览器获取在一系列模块中的标题与内容,以"题目-内容"的字典形式返回
@输入参数: target_index:浏览器编号 / ele_parent:包含标题与内容的模块的元素名 / ele_titles:标题所独有的元素名列表 / 
            long_wait:是否长等待 / ele_chapters:标题共有的章节题目元素名列表 / 
            ele_garbages: 某些需要从整个模块中清除的部分(默认为无)
@返回参数: "题目-内容"的字典形式  是否有章节
@注1:本模块在页面内容方面需要匹配.
@注2:有的内容多了一些项目需要清理,故而存在ele_garbage,作为待选项.
@注3:自动添加text功能.
"""


def browser_find_zip(target_index: int, ele_parent: str, ele_titles: list, long_wait: bool, ele_chapters=None,
                     ele_garbages=None) -> tuple:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    try:
        WebDriverWait(DRIVERS[target_index], wait_time).until(
            ec.presence_of_element_located((By.XPATH, ele_parent.format("1")))
        )
        num_max = len(etree.HTML(DRIVERS[target_index].page_source).xpath(ele_parent[:-4]))
        titles, result, chapters, garbages, has_chapter = [], {}, [], [], False
        page_source = etree.HTML(DRIVERS[target_index].page_source)
        # 对每个整体母模块中,爬出来题目/全部文字
        for i in range(num_max):
            # 先获取所有的题目,这里不一定按顺序
            for ele_title in ele_titles:
                for title in page_source.xpath(ele_parent.format(i + 1) + ele_title + "//text()"):
                    if len(title.strip()) > 0:
                        titles.append(title.strip())
            # 再获取全部内容 包含题目与内容
            contents = page_source.xpath(ele_parent.format(i + 1) + "//text()")
            # 再获取全部章节名
            if ele_chapters is not None:
                for ele_chapter in ele_chapters:
                    for chapter in page_source.xpath(ele_parent.format(i + 1) + ele_chapter + "//text()"):
                        if len(chapter.strip()) > 0:
                            chapters.append(chapter.strip())
            # 再获取垃圾部分内容,待删除
            if ele_garbages is not None:
                for ele_garbage in ele_garbages:
                    for garbage in page_source.xpath(ele_parent.format(i + 1) + ele_garbage + "//text()"):
                        if len(garbage.strip()) > 0:
                            garbages.append(garbage.strip())

            # 从内容部分挨个检索.如果在题目中,则标记为key;如果在垃圾中,则跳过;其余部分则记录
            chapter, key = "", ""
            for content in contents:
                if len(content.strip()) == 0 or content.strip() in garbages:
                    continue
                elif content.strip() in chapters:
                    chapter = content.strip()
                    has_chapter = True
                elif content.strip() in titles:
                    key = content.replace(":", "").strip()
                    if len(chapter.strip()) > 0:
                        key = chapter.strip() + " " + key.strip()
                    # 判断这样的key以前是否出现过,如若出现过,则增添一个括号内容以区分 这部分在韩文里使用
                    if key in result:
                        for j in range(20):
                            if key + "({})".format(j + 1) not in result:
                                key = key + "({})".format(j + 1)
                                break
                    result[key] = ""
                else:  # 内容部分.根据题目下面信息是否已经开始填充判断.
                    if len(result) == 0 or len(result[key]) == 0:
                        result[key] = content.strip()
                    else:
                        result[key] += " " + content.strip()
                    # if key not in result:
                    #     result[key] = content.strip()
                    # else:
                    #     result[key] += " " + content.strip()
        return result, has_chapter
    except (TimeoutException, TypeError):  # 超时/找不到该类模块
        return {}, False
    except Exception as e:  # 找不到该类模块
        print("browser_find_zip中出现了报错.报错类型:{}.报错信息:{}".format(type(e), e))


"""
@模块12:使用指定浏览器获取在一个大模块中的细分标题与内容,以"题目-内容"的字典形式返回.例:Disco网址
"""


def browser_find_zip2(target_index: int, ele_total_parent: str, ele_titles: list, long_wait: bool,
                      ele_garbages=None) -> dict:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    result = {}
    # 等待第一条被发现,开始获取
    WebDriverWait(DRIVERS[target_index], 0.01).until(
        ec.presence_of_element_located((By.XPATH, ele_total_parent))
    )
    # 开始获取
    titles, chapters, garbages, has_chapter = [], [], [], False
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    num_max = len(etree.HTML(DRIVERS[target_index].page_source).xpath(ele_total_parent + ele_titles[0][:-4]))
    # 对每个整体母模块中,爬出来题目/全部文字

    # 先获取所有的题目,这里不一定按顺序
    for ele_title in ele_titles:
        for title in page_source.xpath(ele_total_parent + ele_title[:-4] + "//text()"):
            if len(title.strip()) > 0:
                titles.append(title.strip())
    # 再获取全部内容 包含题目与内容
    contents = page_source.xpath(ele_total_parent + "//text()")
    # 再获取垃圾部分内容,待删除
    if ele_garbages is not None:
        for ele_garbage in ele_garbages:
            for garbage in page_source.xpath(ele_total_parent + ele_garbage + "//text()"):
                if len(garbage.strip()) > 0:
                    garbages.append(garbage.strip())

    # 从内容部分挨个检索.如果在题目中,则标记为key;如果在垃圾中,则跳过;其余部分则记录
    chapter, key = "", ""
    for content in contents:
        if len(content.strip()) == 0 or content.strip() in garbages:
            continue
        elif content.strip() in chapters:
            chapter = content.strip()
            has_chapter = True
        elif content.strip() in titles:
            key = content.strip().replace(":", "")
            if len(chapter.strip()) > 0:
                key = chapter + " " + key
        else:  # 内容部分.根据题目下面信息是否已经开始填充判断.
            if key not in result:
                # 去除控制字符
                for control_character in CONTROL_CHARACTERS:
                    key = key.replace(control_character, "")
                    content = content.replace(control_character, "")
                result[key] = content.strip()
            else:
                # 去除控制字符
                for control_character in CONTROL_CHARACTERS:
                    key = key.replace(control_character, "")
                    content = content.replace(control_character, "")
                result[key] += " " + content.strip()
    return result


"""
@ 模块13:使用指定浏览器获取一个在一系列模块中的标题与内容.但是,当没有序号时,代表是章节题目.
        例:DISCO中序号88985404312(有序号且多题目) && 190758069722(无序号且无题目)028945499628(存在composed作者)
        4053796003232.
    注: 1.判断是否有序列号,可根据是否存在"/td[3]"以判断.
        2.判断当前栏是否为题目,可根据"/td[1]"是否镂空以判断.
        3.composed作者前面需要用" / "以隔开
"""


def browser_find_zip3(target_index: int, ele_parent: str, long_wait: bool) -> str:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    WebDriverWait(DRIVERS[target_index], wait_time).until(
        ec.presence_of_element_located((By.XPATH, ele_parent[:-4]))
    )
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    num_rows = len(page_source.xpath(ele_parent[:-4]))
    has_index = len(page_source.xpath(ele_parent[:-4] + "/td[3]")) > 0
    # 判断是否存在题目,在存在标题之中,是否出现了"XX-XX",且最后一个"-"前不为1.
    many_cd = False
    if has_index:
        first_index = page_source.xpath(ele_parent[:-4] + "/td[1]/text()")
        if "-" in first_index[-1] and first_index[-1].split("-")[0].strip() != "1":
            many_cd = True

    result, chapter_name = "", ""
    # 采用多盘CD的方式记录结果(例:88985404312).即CD之间采用' -- ',内部之间用' ; '
    if many_cd:
        chapter_index = '0'
        for row_num in range(num_rows):
            index = page_source.xpath(ele_parent.format(row_num + 1) + "/td[1]/text()")
            # contents1表示节目单的名字  contents2表示composed by等副标题内容
            contents1 = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/span//text()")
            contents2 = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/blockquote/span//text()")
            time = page_source.xpath(ele_parent.format(row_num + 1) + "/td[3]/span/text()")
            content = ""
            for word in contents1:
                if len(content) > 0:
                    content += " "
                content += word.strip()
            if len(contents2) > 0:
                content += ' / '
                for index2, word in enumerate(contents2):
                    if index2 > 0:
                        content += ' ; '
                    content += word.strip()

            if len(time) > 0:
                time = time[0].strip()

            # 如果第一个位置没有内容,说明是标题
            if len(index) == 0:
                chapter_name = content.strip()
                continue
            index = index[0]
            # 在非标题栏目中,判断是否进入新的CD.
            chapter_now = index[0].split("-")[0].strip()
            if chapter_now != chapter_index:
                chapter_index = chapter_now
                # 之前存在其它章节,加上' -- '
                if len(result) > 0:
                    result += ' -- '
                if len(chapter_name.strip()) > 0:
                    result += "CD {}.{}: ".format(chapter_now, chapter_name.strip())
                else:
                    result += "CD {}: ".format(chapter_now)
            # 直接填入序列号 内容 时间等
            if len(result) > 0 and result[-2:] != ': ':
                result += ' ; '
            if len(time) > 0:
                result += "{}.{} ({})".format(index.replace(".", ""), content.strip(), time.strip())
            else:  # 有的没有时间.例:8051773573637
                result += "{}.{} ".format(index.replace(".", ""), content.strip())
    elif not has_index:
        chapter_index = 0
        # 单盘CD中无序列号模式.(例:190758069722) 使用' -- '切割
        for row_num in range(num_rows):
            contents1 = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/span//text()")
            contents2 = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/blockquote/span//text()")
            time = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/span/text()")
            content = ""
            for word in contents1:
                if len(content) > 0:
                    content += " "
                content += word.strip()
            if len(contents2) > 0:
                content += ' / '
                for index, word in enumerate(contents2):
                    if index > 0:
                        content += ' ; '
                    content += word.strip()
            if len(time) > 0:
                time = time[0].strip()
            chapter_index += 1
            if len(result) > 0:
                result += ' -- '
            if len(time) > 0:
                result += "{}.{} ({})".format(chapter_index, content.strip(), time.strip())
            else:  # 有的没有时间.例:8051773573637
                result += "{}.{} ".format(chapter_index, content.strip())
    else:
        # 单盘CD中,但存在序列号(例:4010276026198)
        chapter_index = '0'
        for row_num in range(num_rows):
            index = page_source.xpath(ele_parent.format(row_num + 1) + "/td[1]/text()")
            # contents1表示节目单的名字  contents2表示composed by等副标题内容
            contents1 = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/span//text()")
            contents2 = page_source.xpath(ele_parent.format(row_num + 1) + "/td[2]/blockquote/span//text()")
            time = page_source.xpath(ele_parent.format(row_num + 1) + "/td[3]/span/text()")
            content = ""
            for word in contents1:
                if len(content) > 0:
                    content += " "
                content += word.strip()
            if len(contents2) > 0:
                content += ' / '
                for index2, word in enumerate(contents2):
                    if index2 > 0:
                        content += ' ; '
                    content += word.strip()

            if len(time) > 0:
                time = time[0].strip()

            # 如果第一个位置没有内容,说明是标题,直接跳过
            if len(index) == 0:
                continue
            index = index[0].strip()
            # 直接填入序列号 内容 时间等
            if len(result) > 0:
                result += ' -- '
            if len(time) > 0:
                result += "{}.{} ({})".format(index.replace(".", ""), content.strip(), time.strip())
            else:  # 有的没有时间.例:8051773573637
                result += "{}.{} ".format(index.replace(".", ""), content.strip())

    result.replace("Composed By - ", "Composed By ")
    return result


"""
@模块14:使用指定浏览器获取一系列模块中的标题与内容.注,这里的题目在抽屉直接的text下,而且抽屉直接的text也只有题目.
例:荷兰网址上面的模块.
"""


def browser_find_zip4(target_index: int, ele_parent: str, long_wait: bool) -> dict:
    global DRIVERS
    wait_time = SINGLE_WAIT_TIME
    if not long_wait:
        wait_time = SHORT_WAIT_TIME
    try:
        WebDriverWait(DRIVERS[target_index], wait_time).until(
            ec.presence_of_element_located((By.XPATH, ele_parent.format("1")))
        )
        num_max = len(etree.HTML(DRIVERS[target_index].page_source).xpath(ele_parent[:-4]))
        titles, result = [], {}
        page_source = etree.HTML(DRIVERS[target_index].page_source)
        # 对每个整体母模块中,爬出来题目/全部文字
        for i in range(num_max):
            # 先获取所有的题目,这里不一定按顺序
            for title in page_source.xpath(ele_parent.format(i + 1) + "/text()"):
                if len(title.strip()) > 0:
                    titles.append(title.strip())
            # 再获取全部内容 包含题目与内容
            contents = page_source.xpath(ele_parent.format(i + 1) + "//text()")

            # 从内容部分挨个检索.如果在题目中,则标记为key;如果在垃圾中,则跳过;其余部分则记录
            key = ""
            for content in contents:
                if len(content.strip()) == 0:
                    continue
                elif content.strip() in titles:
                    key = content.replace(":", "").strip()
                else:  # 内容部分.根据题目下面信息是否已经开始填充判断.
                    if key not in result:
                        result[key] = content.strip()
                    else:
                        result[key] += " " + content.strip()
        return result
    except (TimeoutException, TypeError):  # 超时/找不到该类模块
        return {}
    except Exception as e:  # 找不到该类模块
        print("browser_find_zip3中出现了报错.报错类型:{}.报错信息:{}".format(type(e), e))


"""
@ 模块15:使用指定浏览器获取大模块下的一系列小模块中标题与内容.
例:荷兰网址中下面的详情.
"""


def browser_find_zip5(target_index: int, ele_parent: str, ele_title: str, ele_content: str) -> dict:
    global DRIVERS
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    num, dict_modules, result = len(page_source.xpath(ele_parent[:-4])), {}, {}
    for i in range(num):
        num2 = len(page_source.xpath(ele_parent.format(i + 1) + ele_title[:-4]))
        for j in range(num2):
            key, value = "", ""
            key_list = page_source.xpath(ele_parent.format(i + 1) + ele_title.format(j + 1) + "//text()")
            for key_word in key_list:
                if len(key_word.strip()) > 0:
                    if len(key) > 0:
                        key += " "
                    key += key_word.strip()
            value_list = page_source.xpath(ele_parent.format(i + 1) + ele_content.format(j + 1) + "//text()")
            for value_word in value_list:
                if len(value_word.strip()) > 0:
                    if len(value) > 0:
                        value += " "
                    value += value_word.strip()
            result[key] = value
    return result


"""
@模块16:使用指定浏览器在加载完毕后,根据元素名和指定字符尝试爬取出对应的内容
@输入参数: target_index:浏览器编号/ ele: 指定模块的元素名 / symbol_before: 前方的指定字符 / symbol_after:后方的指定字符
@注1:相较于browser_find_word方法,本方法不需要加载,并且不会因为模块不存在而报错.
@注2:这里symbol_before/symbol_after仅适用于这些符号只出现一次及以下.
"""


def browser_find_word_after_loading(target_index: int, ele: str, symbol_before=None, symbol_after=None) -> str:
    global DRIVERS
    text = etree.HTML(DRIVERS[target_index].page_source).xpath(ele + "/text()")
    result = ""
    for word in text:
        if len(word.strip()) > 0:
            result += word.strip()
    if symbol_before is not None and symbol_before in result:
        result = result[result.find(symbol_before) + 1:]
    if symbol_after is not None and symbol_after in result:
        result = result[result.find(symbol_after) + 1:]
    return result


"""
@模块17:对于给定字符串,空格与逗号后面改成大写,其它给成小写.
@输入参数: words:待转化的字符串
"""


def string_format(words: str) -> str:
    result = ''
    for index, word in enumerate(words):
        try:
            if index == 0:
                result += word.upper()
            elif index == len(words) - 1:
                break
            if word == ' ' or (word in [',', '.'] and words[index + 1] != ' '):
                result += words[index + 1].upper()
            else:
                result += words[index + 1].lower()
        except IndexError:
            result += words[index + 1]
    return result


"""
@模块18:对于给定的245c"XXX XXX XXX"/100a字段人名"XXX,XXX XXX",根据有无逗号生成另一个的字段.
@输入参数: people_name
@返回参数: 包含字段名的字典
注:带逗号的属于100a 不带逗号属于245c
"""


def people_format(people_name: str):
    result, key = {}, ""
    person_name = people_name.split(";")[0].strip()
    if "," in person_name:
        result['100a'] = person_name.strip()
        result['245c'] = (person_name[person_name.find(",") + 1:] + " " + person_name[:person_name.find(",")]).strip()
    else:
        result['245c'] = person_name.strip()
        result['100a'] = (person_name[person_name.rfind(" ") + 1:] + ", " +
                          person_name[:person_name.rfind(" ")]).strip()
    return result


"""
@模块19:对于给定的306a字段,从网站爬下"XX:XX:XX"字符串后,需要将其转化为"XXXXXX"的六位字符串填充306a,
        同时要生成(XX min., XX sec.)填充300a,以list形式返回.
@输入参数:形如"XX:XX:XX"的字符串
@输出参数:由形如"XXXXXX"的六位字符串 与 形如 "(XX min., XX sec.)"的字符串组成的list.
"""


def time_format(time: str) -> list:
    time_branches, result = [0, 0, 0], ["", ""]
    # 获取小时/分钟/秒钟的信息
    try:
        for i in range(3):
            time_branches[-i - 1] = int(time.split(":")[-i - 1].strip())
    except IndexError:
        pass
    # 生成六位字符串
    for i in range(3):
        if len(str(time_branches[i])) < 2:
            result[0] += "0" * (2 - len(str(time_branches[i])))
        result[0] += str(time_branches[i])
    # 生成形如"(XX min., XX sec.)"的字符串
    result[1] = "({} min., {} sec.)".format(time_branches[0] * 60 + time_branches[1], time_branches[2])
    return result


"""
@模块20:节目单格式的整理.
@输入参数: programme:整理前的节目单格式 / has_chapter:是否分CD
    注: 当有多盘CD时:
        1.新的CD用'CD X.'开头2.节目之间用' ; '隔开 3.CD之间用' -- '隔开 
        当只有一盘CD时:
        1.节目之间用' -- '隔开
"""


def programme_format(programmes: dict, has_chapter: bool) -> str:
    result = ""
    if has_chapter:
        # todo 如果是多光盘的部分,如何调整节目单,待处理.
        pass
    else:
        for (k, v) in programmes.items():
            if len(result) > 0:
                result += " -- "
            result += k.strip() + "."
            result += v[:v.rfind(" ") + 1] + "(" + v[v.rfind(" ") + 1:] + ")"
    return result


"""
@ 模块21:生成csv文件
"""


def output_file(file_name: str, mission_name: str):
    global isbn_total, data_total
    result = []
    for isbn in isbn_total:
        if isbn in data_total:
            result.append(data_total[isbn])
    df_ = pd.DataFrame(result)
    if mission_name in ["Amazon", "Disco", "Neitherlands", "Worldcat"]:
        # 如若没有,则增添028a/028b/306a/520/538/700,并在第一行添加'A'
        COLUMNS = ["028a", "028b", "520", "306a", "538", "700"]
        # 显示所有列
        pd.set_option('display.max_columns', None)
        # 显示所有行
        pd.set_option('display.max_rows', None)
        for column in COLUMNS:
            if column not in df_.columns:
                df_[column] = ''
                df_.loc[0, column] = 'A'
    df_.to_csv(file_name, encoding='utf-8')


"""
@ 模块22:Berkeley数据转换格式.
"""


def Berkeley_convert_data(text: str) -> dict:
    result, datas = {}, []
    # 先把网页中显示的多行数据合并为一行
    for words in text.split('\n'):
        if len(words[:3].strip()) == 0:
            datas[-1] = datas[-1].strip() + " " + words.strip()
        else:
            datas.append(words.strip())
    # 再逐行处理
    for data_single in datas:
        tag, indicators, data = "", "", ""
        if data_single[:6] == "LEADER":
            tag = "000"
        else:
            tag = data_single[:3]
        data_single_left = data_single[len(tag) + 1:]
        if tag > "009":
            indicators = data_single_left[:2]
        data_single_left = data_single_left[3:]
        data = data_single_left
        # 如果数据不是以"|"开头,则在其之前增加"|a"
        if tag > '009' and data[0] != "|":
            data = "|a" + data
        # 把数据添加到字典中,先查重.
        if tag in result:
            for i in range(10):
                if "{}({})".format(tag, i) not in result:
                    tag = "{}({})".format(tag, i)
                    break
        result[tag] = indicators + data
    return result


"""
@ 模块23:Yale数据转换格式
"""


def Yale_convert_data(target_index: int) -> dict:
    result = {}
    # 开始整理数据
    WebDriverWait(DRIVERS[target_index], SINGLE_WAIT_TIME).until(
        ec.presence_of_element_located((By.XPATH, "//div[@id='divbib']/ul/li"))
    )
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    data_num = len(page_source.xpath("//div[@id='divbib']/ul/li"))
    for i in range(data_num):
        key_list, value_list = \
            page_source.xpath("//div[@id='divbib']/ul/li[{}]/label//text()".format(i + 1)), \
            page_source.xpath("//div[@id='divbib']/ul/li[{}]/span//text()".format(i + 1))
        key, value = "", ""
        for k in key_list:
            if len(k.strip()) == 0:
                continue
            elif len(key) == 0:
                key = k.strip()
            else:
                value += k.strip().replace("_", " ")
        for v in value_list:
            if len(v.strip()) > 0:
                value += v.strip()
        # 把数据填入字典中.先查重.
        if key in result:
            for j in range(10):
                if "{}({})".format(key, j) not in result:
                    key = "{}({})".format(key, j)
                    break
        result[key] = value
    return result


"""
@ 模块24:British数据转换格式
"""


def British_convert_data(target_index: int) -> dict:
    result = {}
    WebDriverWait(DRIVERS[target_index], SHORT_WAIT_TIME).until(
        ec.presence_of_element_located((By.XPATH, "//table/tbody"))
    )
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    data_num = len(page_source.xpath("//table/tbody/tr/td[@id='bold']"))
    for i in range(data_num):
        key = page_source.xpath("//table/tbody/tr[{}]/td[@id='bold']/text()".format(i + 1))[0]
        value = page_source.xpath("//table/tbody/tr[{}]/td[@class='td1'][2]/text()".format(i + 1))[0].replace(" ",
                                                                                                              " ")
        if key[:3] == "LDR":
            key = '000'
        elif not '0' <= key[0] <= '9':
            continue
        elif key[:3] > '009':
            if len(key) > 3:
                value = key[3:] + " " * (5 - len(key)) + value
            else:
                value = " " * 2 + value
            key = key[:3]
        # 大英网址很特殊,"|x"后大多跟着一个空格.需要清除掉空格.
        while True:
            again = False
            for index, word in enumerate(value):
                if index == len(value) - 2:
                    break
                if word == "|" and value[index + 2] == " ":
                    value = value[:index + 2] + value[index + 3:]
                    again = True
                    break
            if not again:
                break
        if key in result:
            for j in range(10):
                if "{}({})".format(key, j + 1) not in result:
                    key = "{}({})".format(key, j + 1)
                    break
        result[key] = value
    return result


"""
@ 模块25:Michigan数据转换格式
"""


def Michigan_convert_data(target_index: int) -> dict:
    result = {}
    WebDriverWait(DRIVERS[target_index], SINGLE_WAIT_TIME).until(
        ec.presence_of_element_located((By.XPATH, "//tbody/tr/td[@class='marc__field-name']")))
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    data_num = len(page_source.xpath("//tbody/tr/td[@class='marc__field-name']"))
    for i in range(data_num):
        keys = page_source.xpath("//tbody/tr[{}]/td[@class='marc__field-name']//text()".format(i + 1))
        key, value = "", ""
        for words in keys:
            key += words.strip()
        if key == "LDR":
            key = '000'
            values = page_source.xpath("//tbody/tr[{}]/td[2]//text()".format(i + 1))
            for words in values:
                value += words
        elif "0" <= key[0] <= "9":
            if key <= "009":
                values = page_source.xpath("//tbody/tr[{}]/td[4]//text()".format(i + 1))
                for words in values:
                    value += words
            elif key >= "010":
                value += page_source.xpath("//tbody/tr[{}]/td[2]/text()".format(i + 1))[0]
                value += page_source.xpath("//tbody/tr[{}]/td[3]/text()".format(i + 1))[0]
                values = page_source.xpath(
                    "//tbody/tr[{}]/td[4]/span[@class='marc__subfield']//text()".format(i + 1))
                for words in values:
                    value += words.strip()
        if key in result:
            for j in range(10):
                if "{}({})".format(key, j + 1) not in result:
                    key = "{}({})".format(key, j + 1)
                    break
        result[key] = value
    return result


"""
@ 模块26:美图数据转换格式
"""


def US_convert_data(target_index: int) -> dict:
    result = {}
    WebDriverWait(DRIVERS[target_index], SINGLE_WAIT_TIME).until(
        ec.presence_of_element_located((By.XPATH, "//div[@class='top-information-content']")))
    page_source = etree.HTML(DRIVERS[target_index].page_source)
    data_num = len(page_source.xpath("//div[@class='top-information-content']/ul[@id='marc-record']/li"))
    for i in range(data_num):
        keys = page_source.xpath("//ul[@id='marc-record']/li[{}]/span[@class='marc-tag']//text()".format(i + 1))
        key, value = "", ""
        for words in keys:
            key += words.strip()
        if "0" <= key[0] <= "9":
            if key <= "009":
                values = page_source.xpath(
                    "//ul[@id='marc-record']/li[{}]/span[@class='marc-fixed']//text()".format(i + 1))
                for words in values:
                    value += words
            elif key >= "010":
                value += \
                    page_source.xpath(
                        "//ul[@id='marc-record']/li[{}]/span[@class='marc-indicator']/text()".format(i + 1))[
                        0].replace("_", " ")
                values = page_source.xpath(
                    "//ul[@id='marc-record']/li[{}]/span[@class='marc-field']//text()".format(i + 1))
                for words in values:
                    value += words.strip()
            if key in result:
                for j in range(10):
                    if "{}({})".format(key, j + 1) not in result:
                        key = "{}({})".format(key, j + 1)
                        break
        result[key] = value
    return result


"""
@ 模块22:爬虫运行
"""


def crawler_for_cd(input_file: str, output_file: str, thread_num: int, target_name: str, from_csv=True) -> None:
    df = ""
    if from_csv:
        df = pd.DataFrame(get_isbn_from_csv(file_name=input_file)).T
    else:
        df = pd.DataFrame(get_isbn_from_txt(file_name=input_file)).T
    crawlers_queue(thread_num, 0, 0, 600, 1000)
    my_target(thread_num, target_name, output_file)


if __name__ == '__main__':
    crawler_for_cd("朝图1-100.txt", "朝图_US.iso", 4, "US", False)
    # crawler_for_cd("临时测试.txt", "临时测试_British.iso", 1, "British", False)
    # crawler_for_cd("测试数据_cd.csv", "测试书号_cd_Worldcat.csv", 1, "Worldcat", True)
