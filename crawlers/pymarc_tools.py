"""
@encoding:utf-8
@author:Tommy
@time:2020/9/25　17:30
@note:
@备注:
"""
import pandas as pd
from pymarc import MARCReader, Record, Field, MARCWriter
from pymarc.exceptions import NoFieldsFound, RecordLengthInvalid
import os

NON_CHARACTERS_IN_UTF_8 = ["©"]
"""
@模块1:从csv文件生成ISO文件.注意,这里csv文件的列名不一定是按顺序的
"""


def output_iso(file_name: str) -> None:
    output_file_name = file_name[:-4] + ".iso"
    temp_file_name = "临时文件.iso"
    # 先刷新output_file_name
    fp1 = open(output_file_name, 'w', encoding='utf-8')
    fp1.close()
    # 用list-dict显示出来
    dataFrame_temp = pd.read_csv(file_name, encoding='utf-8', dtype=str).to_dict(orient='records')
    dataFrame = []
    # 先把表格中的全部信息录入dataFrame中.注意,如果是nan的部分,则删掉不计入;另,需要删除掉Unnamed列与continue列
    for index, value in enumerate(dataFrame_temp):
        data_single = {}
        for k in value:
            v = str(value[k])
            if v == 'nan' or len(v.strip()) == 0 or "Unnamed" in k or "continue" in k:
                pass
            else:
                data_single[k] = v.strip()
        dataFrame.append(data_single)

    for data in dataFrame:
        record = Record()
        # 先把isbn列筛掉,同时把head列改成000列
        data2 = {}
        for key, value in data.items():
            if key == "head":
                data2["000"] = value
            elif '0' <= key[0] <= '9':
                data2[key] = value

        # 然后对其列进行排序
        keys = list(data2.keys())
        keys.sort()
        # 按照排序后的顺序,逐一进行抓取,并添加入record数据
        for key in keys:
            # 如果是"000",是题名
            if key == "000":
                record.leader = data2[key]
            # 如果是"009"及以内的数据
            elif key <= "009":
                record.add_field(Field(tag=key, data=data2[key]))
            # 如果是"009"以上的数据,需要把"▼"都换成"|",且把第一个"|"之前的数据作为指示符
            elif key > "009":
                # 替换特殊字符
                data2[key] = data2[key].replace("▼", "|")
                # 选中指示位
                indicators = data2[key].split("|")[0]
                if len(indicators) == 0:
                    indicators = [" ", " "]
                elif len(indicators) == 1:
                    indicators = [indicators[0], " "]
                else:
                    indicators = [indicators[0], indicators[1]]
                # 选中数据内容.按照"|"切割,每段"|"之前写两个数据内容
                subfields = []
                for words in data2[key].split("|")[1:]:
                    subfields.append(words[0])
                    subfields.append(words[1:])
                # 加入数据
                record.add_field(Field(tag=key[:3], indicators=indicators, subfields=subfields))

        # 数据生成完毕,写入临时文件
        with open(temp_file_name, 'wb') as fh:
            writer = MARCWriter(fh)
            writer.write(record)
        # 从临时文件录入到生成文件中
        fp1, fp2 = open(temp_file_name, 'r', encoding='utf-8'), open(output_file_name, 'a', encoding='utf-8')
        fp2.write(fp1.readline())
        fp2.write('\n')
        fp1.close()
        fp2.close()
    # 删除临时文件
    os.remove(temp_file_name)


def output_iso_from_data(file_name: str, isbn_total: list, data_total: dict) -> None:
    temp_file_name = "临时文件.iso"
    fp = open(file_name, 'w', encoding='utf-8')
    fp.close()
    records = []
    for isbn in isbn_total:
        record = Record()
        if isbn in data_total:
            data = data_total[isbn]
            for key, value in data.items():
                # 把一些utf8无法识别的符号替换掉.
                for character in NON_CHARACTERS_IN_UTF_8:
                    key, value = str(key).replace(character, ""), str(value).replace(character, "")
                if key in ['continue']:
                    continue
                elif key[:3] == '000':
                    record.leader = value
                elif key[:3] <= '009':
                    record.add_field(Field(tag=key[:3], data=value))
                else:
                    subfields = []
                    words = value[2:].replace("$", " ").replace("|", "$").strip()
                    for word in words.split("$"):
                        if len(word.strip()) == 0:
                            continue
                        else:
                            subfields.append(word.strip()[0])
                            subfields.append(word.strip()[1:])
                    record.add_field(Field(tag=key[:3], indicators=[value[0], value[1]], subfields=subfields))
        if str(record.leader) == str(Record().leader):  # 新的数据
            record.add_field(Field(tag='001', data=isbn))
        record = record_sorted(record)
        records.append(record)

        # 数据生成完毕,写入临时文件
        with open(temp_file_name, 'wb') as fh:
            writer = MARCWriter(fh)
            try:
                writer.write(record)
                # 测试读取是否有问题(如大英9780714827308)
            except UnicodeEncodeError:
                print("编号为:{}的数据格式有误,清空数据以利于输出.".format(isbn))
                record = Record()
                record.add_field(Field(tag='001', data=isbn))
                writer.write(record)

        # 从临时文件录入到生成文件中
        fp1, fp2 = open(temp_file_name, 'r', encoding='utf-8'), open(file_name, 'a', encoding='utf-8')
        try:
            fp2.write(fp1.readline())
        except UnicodeDecodeError:  # 部分解码有误 如大英9780714827308
            fp1.close()
            fp2.close()
            with open(temp_file_name, 'wb') as fh:
                writer = MARCWriter(fh)
                record = Record()
                record.add_field(Field(tag='001', data=isbn))
                writer.write(record)
            fp1, fp2 = open(temp_file_name, 'r', encoding='utf-8'), open(file_name, 'a', encoding='utf-8')
            fp2.write(fp1.readline())
        fp2.write('\n')
        fp1.close()
        fp2.close()

    # 删除临时文件
    os.remove(temp_file_name)


def get_blanks_from_iso(file_name: str, file_name_txt: str) -> None:
    blank_file_name = file_name[:-4] + "_空白isbn.txt"
    data_file_name = file_name[:-4] + "_纯净数据.iso"
    temp_file_name = file_name[:-4] + "_临时数据.iso"
    fp = open(data_file_name, 'w', encoding='utf-8')
    fp.close()
    records, datas, blanks = read_iso(file_name), [], []
    data_nums = get_isbn_from_txt(file_name_txt)

    for index, record in enumerate(records):
        if record_is_blank(record):  # 空白数据.写入"空白_isbn.txt".
            # blanks.append(record.get_fields("001")[0].data)
            blanks.append(data_nums[index])
        else:  # 有效数据.写入"_纯净数据.iso"
            with open(temp_file_name, 'wb') as fh:
                writer = MARCWriter(fh)
                writer.write(record)
            # 从临时文件录入到生成文件中
            fp1, fp2 = open(temp_file_name, 'r', encoding='utf-8'), open(data_file_name, 'a', encoding='utf-8')
            fp2.write(fp1.readline())
            fp2.write('\n')
            fp1.close()
            fp2.close()

    fp = open(blank_file_name, 'w', encoding='utf-8')
    for blank_num in blanks:
        fp.write(blank_num + "\n")
    fp.close()
    os.remove(temp_file_name)


def get_isbn_from_txt(file_name: str) -> list:
    result = []
    # 先把文件中的全部信息录入data_total中.
    fp = open(file_name, 'r', encoding='utf-8')
    words = fp.readline()
    while len(words) > 0:
        if len(words.strip()) > 0:
            # 有的首行有\ufeff,需要清除
            result.append(words.replace("\ufeff", "").strip())
        words = fp.readline()
    return result


# 以Berkeley>Yale>Michigan>British>US
def merge_five_isos(directory_name: str) -> None:
    file_names = ["", "", "", "", ""]
    keywords = {"Berkeley.iso": 0, "Yale.iso": 1, "Michigan.iso": 2, "US.iso": 3, "British.iso": 4}
    for file_name in os.listdir(directory_name):
        for k, v in keywords.items():
            if k == file_name[-len(k):]:
                file_names[v] = file_name
    # 把缺失的文件位置删除.
    for i in range(5):
        if len(file_names[4 - i]) == 0:
            del file_names[4 - i]
    # 依次获取对应文件名下的全部信息.
    datas, records = [], []
    for file_name in file_names:
        datas.append(read_iso(directory_name + "\\" + file_name))
    data_num = len(datas[0])
    for i in range(data_num):
        # 依次从各个文件寻找.
        for j in range(len(datas)):
            # 如果找到
            if not record_is_blank(datas[j][i]):
                records.append(datas[j][i])
                break
            # 如果没有找到,增加一个空白数据
            elif j == len(datas) - 1:
                records.append(datas[-1][i])
    output_file_name = directory_name + "\\" + "五合一.iso"
    temp_file_name = "临时文件.iso"
    # 先刷新output_file_name
    fp1 = open(output_file_name, 'w', encoding='utf-8')
    fp1.close()
    for index, record in enumerate(records):
        # 数据生成完毕,写入临时文件
        with open(temp_file_name, 'wb') as fh:
            writer = MARCWriter(fh)
            writer.write(record)
        # 从临时文件录入到生成文件中
        fp1, fp2 = open(temp_file_name, 'r', encoding='utf-8'), open(output_file_name, 'a', encoding='utf-8')
        fp2.write(fp1.readline())
        fp2.write('\n')
        fp1.close()
        fp2.close()
    # 删除临时文件
    os.remove(temp_file_name)


"""
@模块2:从list-record中输出iso文件.
"""


def output_iso_from_iso(output_file_name: str, records: list) -> None:
    output_file_name = output_file_name[:-4] + ".iso"
    temp_file_name = "临时文件.iso"
    # 先刷新output_file_name
    fp1 = open(output_file_name, 'w', encoding='utf-8')
    fp1.close()
    for index, record in enumerate(records):
        # 数据生成完毕,写入临时文件
        with open(temp_file_name, 'wb') as fh:
            writer = MARCWriter(fh)
            writer.write(record)
        # 从临时文件录入到生成文件中
        fp1, fp2 = open(temp_file_name, 'r', encoding='utf-8'), open(output_file_name, 'a', encoding='utf-8')
        fp2.write(fp1.readline())
        fp2.write('\n')
        fp1.close()
        fp2.close()
    # 删除临时文件
    os.remove(temp_file_name)


"""
@模块3:从iso文件中逐行读取数据.返回list(Record)形式
"""


def read_iso(file_name: str) -> list:
    result = []
    temp_name = "临时.iso"
    # 读入数据
    fp = open(file_name, 'r', encoding='utf-8')
    for index, data in enumerate(fp):
        # 把当前这行数据写入临时文件
        # try:
        fp_temp = open(temp_name, 'w', encoding='utf-8')
        fp_temp.write(data)
        fp_temp.close()
        # 用marc形式读取
        fh = open(temp_name, 'rb')
        try:
            reader = MARCReader(fh)
            record = next(reader)
        except (NoFieldsFound, UnicodeDecodeError):  # 如果未从网站爬下,存在使用无内容的数据占位的数据.仍用无内容的数据补位.
            record = Record()
        except RecordLengthInvalid:  # 读取数据多了最后一行的回车符,则跳出
            break
        finally:
            fh.close()
            result.append(record)
    fp.close()
    os.remove(temp_name)

    return result


def record_is_blank(record: Record) -> bool:
    return str(record.leader) == "00052     2200037   4500" or str(record.leader) == str(Record().leader)


"""
@模块4:指定字段,指定字符串进行替代.
"""


def field_replace_substring(record: Record, field_name: str, keywords_before: str, keywords_after: str,
                            is_indicators=False, all_indicators=False, subfield_name=None, index=-1) -> Record:
    result = record
    if field_name in ['000', 'LDR']:
        result.leader = result.leader.replace(keywords_before, keywords_after)
    else:
        # 先寻出旧数据中所有的字段名下字段
        old_fields = result.get_fields(field_name)
        # 删除旧的字段
        result.remove_fields(field_name)
        if int(field_name) <= 9:
            # 依次增添新的字段
            for field in old_fields:
                result.add_field(Field(tag=field.tag, data=field.data.replace(keywords_before, keywords_after)))
        else:
            # 依此增添新的字段
            for field in old_fields:
                if is_indicators:  # 修改指示符
                    if all_indicators or field.indicators == list(keywords_before):
                        result.add_field(
                            Field(tag=field.tag, indicators=list(keywords_after), subfields=field.subfields))
                    else:
                        result.add_field(field)
                else:  # 修改内容
                    if subfield_name is None:  # 在全子字段中搜索
                        total_words = ""
                        for index2, words in enumerate(field.subfields):
                            if index2 % 2 == 0:
                                total_words += "$"
                            total_words += str(words)
                        subfields_temp = total_words.replace(keywords_before, keywords_after)
                        subfields_temp2 = []
                        for index2, words in enumerate(subfields_temp.split("$")):
                            if index2 == 0 and len(words.strip()) == 0:
                                continue
                            else:
                                subfields_temp2.append(words[0])
                                subfields_temp2.append(words[1:])
                        result.add_field(Field(tag=field.tag, indicators=field.indicators, subfields=subfields_temp2))
                    else:  # 在指定子字段中搜索
                        total_words = ""
                        for index2 in range(len(field.subfields)):
                            words = field.subfields[index2]
                            if index2 % 2 == 0:
                                total_words += "$"
                            elif field.subfields[index2 - 1] == subfield_name:
                                words = words.replace(keywords_before, keywords_after)
                            total_words += words
                        subfields_temp = total_words
                        subfields_temp2 = []
                        for index2, words in enumerate(subfields_temp.split("$")):
                            if index2 == 0 and len(words.strip()) == 0:
                                continue
                            else:
                                subfields_temp2.append(words[0])
                                subfields_temp2.append(words[1:])
                        result.add_field(Field(tag=field.tag, indicators=field.indicators, subfields=subfields_temp2))

    return result


"""
@模块5:删除某个字段中的子字段/整个字段.
注:对subfield_name为"",则删除整个字段
"""


def field_remove_subfield(record: Record, field_name: str, subfield_name: str) -> Record:
    result = record

    # 先寻出旧数据中所有的字段名下字段
    old_fields = result.get_fields(field_name)
    # 删除旧的字段
    result.remove_fields(field_name)

    # 如果剩余字段存在内容,则把剩余的字段添加回去
    if len(subfield_name) == 1:
        for field in old_fields:
            field.delete_subfield(subfield_name)
            if len(field.subfields) > 0:
                result.add_field(field)

    return result


"""
@模块6:添加固定内容的子字段.
"""


def field_add(record: Record, field_name: str, data=str, indicators: str = None, new_field=True):
    result = record
    if new_field:
        if indicators is None:
            result.add_field(Field(tag=field_name, data=data))
        else:
            subfields = []
            for index, words in enumerate(data.split("$")):
                if index == 0:
                    continue
                subfields.append(words[0])
                subfields.append(words[1:])
            result.add_field(Field(tag=field_name, indicators=[indicators[0], indicators[1]], subfields=subfields))
    else:
        # 先寻出旧数据中所有的字段名下字段
        old_fields = result.get_fields(field_name)
        if len(old_fields) > 0:
            # 删除旧的字段
            result.remove_fields(field_name)
            for old_field in old_fields:
                subfields = old_field.subfields
                for index, words in enumerate(data.split("$")):
                    if index == 0:
                        continue
                    subfields.append(words[0])
                    subfields.append(words[1:])
                result.add_field(Field(tag=field_name, indicators=old_field.indicators, subfields=subfields))
        else:
            result = field_add(result, field_name, data, indicators, True)
    return result


"""
@模块7:拷贝子字段内容
"""


def field_copy(record: Record, field_name1: str, subfield_name1: str, field_name2: str, subfield_name2: str,
               new_indicators=None, old_indicators=None, index=-1):
    result = record
    # 先寻出旧数据中所有的字段名下字段
    old_fields = result.get_fields(field_name1)

    for old_field in old_fields:
        subfields = old_field.get_subfields(subfield_name1)
        # 如果是拷贝整个字段
        if subfield_name1 == "" and subfield_name2 == "":
            if old_indicators == None:
                if new_indicators == None and len(old_field.subfields) > 0:
                    result.add_field(
                        Field(tag=field_name2, indicators=old_field.indicators, subfields=old_field.subfields))
                elif len(old_field.subfields) > 0:
                    result.add_field(Field(tag=field_name2, indicators=new_indicators, subfields=old_field.subfields))
            else:
                if old_indicators == old_field.indicators and len(old_field.subfields) > 0:
                    result.add_field(Field(tag=field_name2, indicators=new_indicators, subfields=old_field.subfields))
                elif len(old_field.subfields) > 0:
                    result.add_field(
                        Field(tag=field_name2, indicators=old_field.indicators, subfields=old_field.subfields))
        # 如果只是拷贝子字段
        else:
            indicators, indicators2 = "", ""
            for word in old_field.indicators:
                indicators += word
            for word in new_indicators:
                indicators2 += word
            subfields2 = []
            for subfield in subfields:
                subfields2.append(subfield_name2)
                subfields2.append(subfield)
            subfields2_str = ""
            for index, word in enumerate(subfields2):
                if index % 2 == 0:
                    subfields2_str += "$"
                subfields2_str += word
            if old_indicators == None:  # 无指定匹配指示符
                if new_indicators is None:
                    if len(subfields2_str) > 0:
                        result = field_add(result, field_name2, subfields2_str, indicators=indicators)
                else:
                    if len(subfields2_str) > 0:
                        result = field_add(result, field_name2, subfields2_str, indicators2)
            else:
                # 旧的吻合,则更改
                if indicators == old_indicators:
                    if len(indicators2) > 0:
                        result = field_add(result, field_name2, subfields2_str, indicators2)
                else:  # 旧的不吻合,则沿用之前的
                    if len(indicators) > 0:
                        result = field_add(result, field_name2, subfields2_str, indicators=indicators)
    return result


"""
@模块8:指定字段结尾增添符号.只针对'010'及以上字段.
"""


def field_add_symbol(record: Record, field_name: str, subfield_name: str, symbol: str) -> Record:
    result = record

    # 先寻出旧数据中所有的字段名下字段
    old_fields = result.get_fields(field_name)
    # 删除旧的字段
    result.remove_fields(field_name)

    # 如果剩余字段存在内容,则把剩余的字段添加回去
    for field in old_fields:
        subfields = []
        subfields.append(field.subfields[0])
        for index, subfield in enumerate(field.subfields):
            if index == len(field.subfields) - 1:
                if subfield == '':
                    subfields[-1] += symbol
                break
            # 非通用符且寻找到关键词
            if subfield != '' and subfield == subfield_name:
                subfields.append(field.subfields[index + 1] + symbol)
            else:
                subfields.append(field.subfields[index + 1])
        result.add_field(Field(tag=field.tag, indicators=field.indicators, subfields=subfields))
    return result


"""
@模块9:指定字段中是否包含关键字符串
"""


def field_contains_keywords(record: Record, field_name: str, is_indicator: bool = False,
                            keywords=None, subfield_name: str = None) -> bool:
    result = record
    if is_indicator:
        # 先寻出旧数据中所有的字段名下字段
        old_fields = result.get_fields(field_name)
        for old_field in old_fields:
            if old_field.indicators == keywords:
                return True
        return False
    else:
        # 先寻出旧数据中所有的字段名下字段
        old_fields = result.get_fields(field_name)
        for index, old_field in enumerate(old_fields):
            # 只搜查某一个字段
            if len(subfield_name) == 1:
                subfields = ""
                for index2, subfield in enumerate(old_field.subfields):
                    if index2 == len(old_field.subfields) - 1:
                        break
                    if subfield_name is None or subfield_name == subfield:
                        if keywords in old_field.subfields[index2 + 1]:
                            return True
            else:  # 搜查全字段
                if keywords in str(old_field):
                    return True

        return False


"""
@模块10:替换前几个的字段名,默认为-1全部.
"""


def record_replace_fields(record: Record, old_field_name: str, new_field_name: str,
                          replace_num_list: list = [], index=-1) -> Record:
    result = record
    # 先寻出旧数据中所有的字段名下字段
    old_fields = result.get_fields(old_field_name)
    for index, old_field in enumerate(old_fields):
        if index in replace_num_list or replace_num_list == []:
            result.remove_field(old_field)
            result.add_field(Field(tag=new_field_name, indicators=old_field.indicators, subfields=old_field.subfields))
    return result


"""
@模块11:让字段排序.
"""


def record_sorted(record: Record) -> Record:
    result = Record()
    result.leader = record.leader
    for i in range(1000):
        field_name = str(i)
        while len(field_name) < 3:
            field_name = "0" + field_name
        # 先寻出旧数据中所有的字段名下字段
        old_fields = record.get_fields(field_name)
        for field in old_fields:
            result.add_field(field)
    return result


"""
@模块12:封装好的韩文规范流程.
"""


def South_Korea_format(input_file_name: str, output_file_name: str) -> None:
    data_total = read_iso(input_file_name)
    for index, record in enumerate(data_total):
        try:
            result = record
            # 1.头标的"p"换成"n"
            result = field_replace_substring(result, '000', 'p', 'n')
            # 2.头标的"c"换成"a"
            result = field_replace_substring(result, '000', 'c', 'a')
            # 3.008的"kor"换成"kor d"
            result = field_replace_substring(result, '008', 'kor', 'kor d')
            # 4.008的"ulk"/"ggk"/"tjk"换成"ko "       "000   "换成"000 0 "           "001   "换成"001 0 "
            result = field_replace_substring(result, '008', 'ulk', 'ko ')
            result = field_replace_substring(result, '008', 'ggk', 'ko ')
            result = field_replace_substring(result, '008', 'tjk', 'ko ')
            result = field_replace_substring(result, '008', '000   ', '000 0 ')
            result = field_replace_substring(result, '008', "001   ", "001 0 ")
            # 5.删除子字段020c,020g
            result = field_remove_subfield(result, '020', 'c')
            result = field_remove_subfield(result, '020', 'g')
            # 6.删除字段007, 023, 049, 830, 950.
            result = field_remove_subfield(result, '007', '')
            result = field_remove_subfield(result, '023', '')
            result = field_remove_subfield(result, '049', '')
            result = field_remove_subfield(result, '830', '')
            result = field_remove_subfield(result, '950', '')
            # 7.020指示符"1#"替换成"##"
            result = field_replace_substring(result, '020', '1 ', '  ', True)
            # 8.020字段"#("替换成"$q"
            result = field_replace_substring(result, '020', ' (', '$q')
            # 9.020字段")"替换成"#"
            result = field_replace_substring(result, '020', ')', ' ')
            # 10.添加字段040  $aZTJCKTS$cZTJCKTS
            result = field_add(result, '040', "$aZTJCKTS$cZTJCKTS", "  ")
            # 11.添加字段093  $aA$25
            result = field_add(result, '093', "$aA$25", "  ")
            # 12.245字段"/$d"替换成245"/$c"
            result = field_replace_substring(result, "245", "/$d", "/$c")
            # 13.245c中, "#;$e"替换成"#;#", ",$e"替换成",#"
            result = field_replace_substring(result, "245", ";$e", " ; ")
            result = field_replace_substring(result, "245", ",$e", ", ")
            # 14.505字段中"$t"替换成"$a",  "#--$a"替换成"#;#",   505指示符"00"替换成"0#"
            result = field_replace_substring(result, "505", "$t", "$a")
            result = field_replace_substring(result, "505", "--$a", " ; ")
            result = field_replace_substring(result, "505", "00", "0 ", True)
            # 15. 700字段的子字段4删除, 指示符为"1#"
            result = field_remove_subfield(result, "700", "4")
            result = field_replace_substring(result, "700", "  ", "1 ", True, all_indicators=True)
            # 16. 710字段指示符"##"替换成"2#"
            result = field_replace_substring(result, "710", "  ", "2 ", True)
            # 17. “245x”复制粘贴到"246a",指示符为"31".然后把"245x"替换成"245b"
            #     如果此字段存在2个"$b", 那么"=$b"替换成"=#"
            result = field_copy(result, '245', 'x', '246', 'a', ['3', '1'], index=index)
            result = field_replace_substring(result, "245", '$x', '$b')
            result = field_replace_substring(result, '245', '=$b', '= ')
            # 18. 子字段245c, 260c,300c, 710a, 740a末尾加点“.“
            result = field_add_symbol(result, '245', 'c', '.')
            result = field_add_symbol(result, '300', 'c', '.')
            result = field_add_symbol(result, '710', 'a', '.')
            result = field_add_symbol(result, '740', 'a', '.')
            # 19. 如果子字段245c中有"지은이"/"#지음"/"글:#", 则第一个700替换成100， 随即245指示符"00"替换成"10", 100指示符"0#"替换成"1#"  （见数据9791155160176）
            if field_contains_keywords(result, "245", False, "지은이", 'c') \
                    or field_contains_keywords(result, "245", False, " 지음", 'c') \
                    or field_contains_keywords(result, "245", False, "글: ", 'c'):
                result = record_replace_fields(result, "700", "100", [0])
                result = field_replace_substring(result, "245", "00", "10", True, False)
                result = field_replace_substring(result, "100", "0 ", "1 ", True, False)
            # 20. 246字段指示符为19时：
            #                246字段更名为240，指示符19替换成10， 此时增加子字段240l“Korean”, 随即245指示符"00"替换成"10"  (见数据9788901178479第）
            if field_contains_keywords(result, "246", True, ['1', '9']):
                result = record_replace_fields(result, "246", "240", index=10)
                result = field_replace_substring(result, "240", "19", "10", True)
                result = field_add(result, "240", "$lKorean", new_field=False)
                result = field_replace_substring(result, "245", "00", "10", True)
            # 21."240"字段中"#:$b"换成":#"   "$l"换成".$l"
            result = field_replace_substring(result, "240", " :$b", ": ", index=index)
            result = field_replace_substring(result, "240", "$l", ".$l")
            # 22."440"替换成"490", 指示符"00"替换成"0#"
            result = field_copy(result, '440', '', '490', '', ['0', ' '], ['0', '0'])
            result = field_remove_subfield(result, '440', '')
            # 23.490字段指示符"10"替换成"0#"
            result = field_replace_substring(result, "490", "10", "0 ", True)
            # 24."940a"替换成"246a", 指示符"0#"替换成"3#"
            result = field_copy(result, '940', 'a', '246', 'a', ['3', ' '], ['0', ' '])
            result = field_remove_subfield(result, '940', 'a')
            # 25."246a"中" /"换成"  " , 246字段中"#:#"替换成"#:$b"
            result = field_replace_substring(result, "246", " /", "  ", subfield_name='a')
            result = field_replace_substring(result, "246", " : ", " :$b")
            # 26.490字段指示符"10"替换成"0#"  “00"替换成"0#"
            result = field_replace_substring(result, "490", "10", "0 ", True, index=index)
            result = field_replace_substring(result, "490", "00", "0 ", True)
            # 27.260c子字段末尾加点"."      500a子字段末尾加点"."
            result = field_add_symbol(result, "260", 'c', '.')
            result = field_add_symbol(result, "500", 'a', '.')
            # 28.若单条700d子字段不存在, 700a子字段末尾加点"."
            old_fields = result.get_fields("700")
            result.remove_fields("700")
            for old_field in old_fields:
                if len(old_field.get_subfields('d')) > 0:
                    result.add_field(old_field)
                else:
                    subfields = old_field.subfields
                    for index2, subfield in enumerate(subfields):
                        if subfield == 'a':
                            subfields[index2 + 1] += '.'
                    result.add_field(Field(tag=old_field.tag, indicators=old_field.indicators, subfields=subfields))
            # 29.删除006字段
            result = field_remove_subfield(result, "006", '')
            # 30.020指示符"1#"替换成"##"
            result = field_replace_substring(result, '020', '1 ', '  ', True)
            result = record_sorted(result)
            data_total[index] = result
        except AttributeError:  # 有时候没有查询下来的数据,修改时会报错
            print("第{}条数据转换失败.如该条数据为空白,则忽略此信息.".format(index + 1))
    output_iso_from_iso(output_file_name, data_total)


"""
@模块13:按照020中的"1-1000"字段顺序排序.
"""


def record_sorted_020(input_file_name: str, output_file_name: str) -> None:
    # 先从指定位置读取数据
    datas = read_iso(input_file_name)
    results = {}
    results_list = []
    for index, data in enumerate(datas):
        # 尝试获取020字段
        subfields = data.get_fields('020')
        for subfield in subfields:
            key = subfield.get_subfields('a')
            if len(key) > 0:
                key = key[0]
                if len(key) <= 3:
                    if int(key) not in results:
                        results[int(key)] = [data]
                    else:
                        results[int(key)].append(data)
    # 从小到大排序
    results_keys = results.keys()
    sorted_results_keys = sorted(results_keys)
    for key in sorted_results_keys:
        for record in results[key]:
            results_list.append(record)
    # 输出
    output_iso_from_iso(output_file_name, results_list)


if __name__ == '__main__':
    # directory = "C:\\Users\\Administrator\\PycharmProjects\\GlobalBibliography\\crawlers"
    # merge_five_isos(directory)
    directory = "C:\\Users\\Administrator\\Desktop\\待排序数据201115.iso"
    directory2 = "C:\\Users\\Administrator\\Desktop\\待排序数据201115_转化后.iso"
    record_sorted_020(directory, directory2)
