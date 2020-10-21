"""
@encoding:utf-8
@author:Tommy
@time:2020/10/15　8:58
@note:
@备注:
"""
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QComboBox, QLabel, QTextEdit, QFileDialog
from PyQt5 import QtGui, QtWidgets
from os import listdir
import pandas as pd


data_total = []
dfs = ['', '', '', '', '', '', '', '']
total_columns = set()  # 获取列名并集
current_column = ''
data_single = {}
output_file = ''


class Window(QWidget):
    # 初始化窗口,设置窗口的尺寸与位置。默认设置为3000*1800/1500*800的尺寸。同时设置四个区域(1列表区,2预览区,3流程区,4按钮区)
    def __init__(self):
        super().__init__()
        self.setWindowTitle("书目分类软件")

        # 调整大小(上面是奔驰妈的尺寸，下是老妈的尺寸)
        # self.resize_x, self.resize_y = 680, 768
        self.resize_x, self.resize_y = 1500, 1080
        self.resize(self.resize_x, self.resize_y)

        # 调整初始显示位置
        self.move(0, 50)

        # 初始化选区
        self.widget1 = self.add_widget('dark', self.resize_x, self.resize_y, 0, 0)

        self.init_widget1()

    # 初始化列表区,1区
    def init_widget1(self):
        global data_total, dfs, total_columns, current_column, output_file
        #       第一列
        # 标志为"目标字段"的辅助提示
        self.add_label(self.widget1, "目标字段", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 1 / 18, self.widget1.resize_y * 1 / 60)
        # 标志为"下一条"的按钮
        button9 = self.add_button(self.widget1, "下一条", self.widget1.resize_x * 1 / 9, self.widget1.resize_y * 3 / 60,
                                  self.widget1.resize_x * 3 / 18, self.widget1.resize_y * 40 / 60)
        # 标志为"上一条"的按钮
        button10 = self.add_button(self.widget1, "上一条", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                   self.widget1.resize_x * 1 / 18, self.widget1.resize_y * 40 / 60)
        # 标志为"目标字段文字框"的文字框
        self.add_text(self.widget1, '目标字段文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 35 / 60,
                      self.widget1.resize_x * 1 / 18, self.widget1.resize_y * 4 / 60)

        # 添加提示框，标记当前isbn编号与字段
        self.add_label(self.widget1, "当前字段信息", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 1 / 18, self.widget1.resize_y * 45 / 60)

        # 添加提示框，标记当前isbn编号与字段
        self.add_text(self.widget1, "字段信息文字框", self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 10 / 60,
                      self.widget1.resize_x * 1 / 18, self.widget1.resize_y * 48 / 60)
        #       第二列
        # 标志为"迪斯科"的辅助提示
        self.add_label(self.widget1, "迪斯科", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 1 / 60)
        # 标志为"拷贝迪斯科"的按钮
        button1 = self.add_button(self.widget1, "拷贝迪斯科", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 8 / 18, self.widget1.resize_y * 1 / 60)
        # 标志为"迪斯科文字框"的文字框
        self.add_text(self.widget1, '迪斯科文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 15 / 60,
                      self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 4 / 60)

        # 标志为"迪斯科_谷歌"的辅助提示
        self.add_label(self.widget1, "迪斯科_谷歌", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 19 / 60)
        # 标志为"迪斯科_谷歌"的按钮
        button4 = self.add_button(self.widget1, "拷贝迪斯科谷歌", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 8 / 18, self.widget1.resize_y * 19 / 60)
        # 标志为"迪斯科_谷歌文字框"的文字框
        self.add_text(self.widget1, '迪斯科_谷歌文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 8 / 60,
                      self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 22 / 60)

        # 标志为"荷兰"的辅助提示
        self.add_label(self.widget1, "荷兰", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 30 / 60)
        # 标志为"拷贝荷兰"的按钮
        button2 = self.add_button(self.widget1, "拷贝荷兰", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 8 / 18, self.widget1.resize_y * 30 / 60)
        # 标志为"荷兰文字框"的文字框
        self.add_text(self.widget1, '荷兰文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 15 / 60,
                      self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 33 / 60)

        # 标志为"荷兰_谷歌"的辅助提示
        self.add_label(self.widget1, "荷兰_谷歌", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 48 / 60)
        # 标志为"荷兰_谷歌"的按钮
        button5 = self.add_button(self.widget1, "拷贝荷兰谷歌", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 8 / 18, self.widget1.resize_y * 48 / 60)
        # 标志为"荷兰_谷歌文字框"的文字框
        self.add_text(self.widget1, '荷兰_谷歌文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 8 / 60,
                      self.widget1.resize_x * 6 / 18, self.widget1.resize_y * 51 / 60)

        #       第三列
        # 标志为"亚马逊"的辅助提示
        self.add_label(self.widget1, "亚马逊", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 1 / 60)
        # 标志为"拷贝亚马逊"的按钮
        button7 = self.add_button(self.widget1, "拷贝亚马逊", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 13 / 18, self.widget1.resize_y * 1 / 60)
        # 标志为"亚马逊文字框"的文字框
        self.add_text(self.widget1, '亚马逊文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 15 / 60,
                      self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 4 / 60)
        # 标志为"亚马逊_谷歌"的辅助提示
        self.add_label(self.widget1, "亚马逊_谷歌", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 19 / 60)
        # 标志为"亚马逊_谷歌"的按钮
        button8 = self.add_button(self.widget1, "拷贝亚马逊谷歌", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 13 / 18, self.widget1.resize_y * 19 / 60)
        # 标志为"亚马逊_谷歌文字框"的文字框
        self.add_text(self.widget1, '亚马逊_谷歌文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 8 / 60,
                      self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 22 / 60)
        # 标志为"世界猫"的辅助提示
        self.add_label(self.widget1, "世界猫", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 30 / 60)
        # 标志为"拷贝世界猫"的按钮
        button3 = self.add_button(self.widget1, "拷贝世界猫", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 13 / 18, self.widget1.resize_y * 30 / 60)
        # 标志为"世界猫文字框"的文字框
        self.add_text(self.widget1, '世界猫文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 15 / 60,
                      self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 33 / 60)
        # 标志为"世界猫_谷歌"的辅助提示
        self.add_label(self.widget1, "世界猫_谷歌", self.widget1.resize_x * 2 / 9, self.widget1.resize_y / 20,
                       self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 48 / 60)
        # 标志为"世界猫_谷歌"的按钮
        button6 = self.add_button(self.widget1, "拷贝世界猫谷歌", self.widget1.resize_x * 1 / 9, self.widget1.resize_y / 20,
                                  self.widget1.resize_x * 13 / 18, self.widget1.resize_y * 48 / 60)
        # 标志为"世界猫_谷歌文字框"的文字框
        self.add_text(self.widget1, '世界猫_谷歌文字框', self.widget1.resize_x * 2 / 9, self.widget1.resize_y * 8 / 60,
                      self.widget1.resize_x * 11 / 18, self.widget1.resize_y * 51 / 60)

        button1.clicked.connect(self.button1_action)
        button2.clicked.connect(self.button2_action)
        button3.clicked.connect(self.button3_action)
        button4.clicked.connect(self.button4_action)
        button5.clicked.connect(self.button5_action)
        button6.clicked.connect(self.button6_action)
        button7.clicked.connect(self.button7_action)
        button8.clicked.connect(self.button8_action)
        button9.clicked.connect(self.button9_action)
        button10.clicked.connect(self.button10_action)

        # 弹出选择文件夹的提示
        directory = QFileDialog.getExistingDirectory(self, "请选取待批量处理的文件夹", "./")
        # 抓取该文件夹下所有文件的路径
        files = map(lambda x: directory + "/" + x, listdir(directory))
        dict_ = {0: "_迪斯科", 1: "_荷兰", 2: "_世界猫", 3: "_迪斯科_谷歌", 4: "_荷兰_谷歌", 5: "_世界猫_谷歌", 6: "_亚马逊", 7: "亚马逊_谷歌"}
        for file in files:
            for key in dict_:
                value = dict_[key]
                if file[-4 - len(value):-4] == value:
                    dfs[key] = pd.read_csv(file, encoding='utf-8', dtype=str, index_col=False)
                    total_columns = total_columns | set(dfs[key].columns)
                    output_file = file[:-4 - len(value)] + "_软件生成.csv"
            # 如果之前已经有数据存储,则读取并抛弃最后一条不完整数据
            if file[-9:-4] == '_软件生成':
                data_total = pd.read_csv(file, encoding='utf-8', dtype=str, index_col=False)
                data_total = data_total.to_dict(orient='records')
                # if len(data_total) >= 1:
                #     data_total.pop()
        total_columns = list(total_columns)
        total_columns.sort()
        current_column = total_columns[0]
        # 先把当前条数与列名显示在9号框中
        self.setText(9, "条数:{}/{}".format(len(data_total) + 1, len(dfs[0])) + "\n" + "列名顺序:{}/{}".format(
            total_columns.index(current_column), len(total_columns)) + "\n" + "列名:{}".format(current_column))
        # 再分别把这8个表格的信息显示在对应方框中
        for i in range(8):
            if current_column in dfs[i].columns:
                data_temp = str(dfs[i].loc[len(data_total), current_column])
                if data_temp == 'nan':
                    self.setText(i, "")
                else:
                    self.setText(i, data_temp)
            else:
                self.setText(i, "")
        # 显示初始字段
        for data in data_total:
            print(data)

    # 增添区域
    def add_widget(self, color, resize_x, resize_y, move_x, move_y):
        widget = QWidget(self)
        widget.setStyleSheet("background-color: {};".format(color))
        widget.resize(resize_x, resize_y)
        widget.resize_x, widget.resize_y, widget.move_x, widget.move_y = resize_x, resize_y, move_x, move_y
        widget.move(move_x, move_y)
        return widget

    # 在区域中增添点击按钮,并设置尺寸与摆放位置
    @staticmethod
    def add_button(widget, title, resize_x, resize_y, move_x, move_y):
        button_ = QPushButton(widget)
        # button_.setParent(widget)
        button_.setText(title)
        button_.setObjectName(title)
        button_.resize(round(resize_x), round(resize_y))
        button_.move(round(move_x), round(move_y))
        return button_

    # 增添单行文字提示
    @staticmethod
    def add_label(widget, text, resize_x, resize_y, move_x, move_y):
        label = QLabel(widget)
        label.setObjectName(text)
        label.setText(text)
        label.resize(round(resize_x), round(resize_y))
        label.move(round(move_x), round(move_y))

    # 增添多行文字提示
    @staticmethod
    def add_text(widget, text, resize_x, resize_y, move_x, move_y):
        text_edit = QTextEdit(widget)
        text_edit.setObjectName(text)
        text_edit.setText(text)
        text_edit.resize(round(resize_x), round(resize_y))
        text_edit.move(round(move_x), round(move_y))
        font = QtGui.QFont()
        font.setFamily("Microsoft YaHei")
        font.setPointSize(12)
        text_edit.setFont(font)

    # "拷贝迪斯科"按钮点击后执行的操作
    def button1_action(self):
        edittext = self.findChild(QTextEdit, "迪斯科文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝荷兰"按钮点击后执行的操作
    def button2_action(self):
        edittext = self.findChild(QTextEdit, "荷兰文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝世界猫"按钮点击后执行的操作
    def button3_action(self):
        edittext = self.findChild(QTextEdit, "世界猫文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝迪斯科谷歌"按钮点击后执行的操作
    def button4_action(self):
        edittext = self.findChild(QTextEdit, "迪斯科_谷歌文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝荷兰谷歌"按钮点击后执行的操作
    def button5_action(self):
        edittext = self.findChild(QTextEdit, "荷兰_谷歌文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝世界猫谷歌"按钮点击后执行的操作
    def button6_action(self):
        edittext = self.findChild(QTextEdit, "世界猫_谷歌文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝亚马逊"按钮点击后执行的操作
    def button7_action(self):
        edittext = self.findChild(QTextEdit, "亚马逊文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "拷贝亚马逊谷歌"按钮点击后执行的操作
    def button8_action(self):
        edittext = self.findChild(QTextEdit, "亚马逊_谷歌文字框")
        edittext2 = self.findChild(QTextEdit, "目标字段文字框")
        edittext2.setText(edittext.toPlainText())

    # "下一条"按钮点击后执行的操作
    def button9_action(self):
        global data_single, current_column, total_columns, data_total, output_file
        #   先将当前字段信息保留到data_single中
        data_single[current_column] = self.findChild(QTextEdit, "目标字段文字框").toPlainText().replace("\n", "\t")
        #   判断current_column是否为total_columns最后一个,如是,添加到data_total中,并生成文件。
        if current_column == total_columns[-1]:
            data_total.append(data_single)
            data_single = {}
            pd.DataFrame(data_total).to_csv(output_file, encoding='utf-8', index=False)
            current_column = total_columns[0]
        #   current_column自增。显示对应字段信息,更新方框9的内容
        else:
            current_column = total_columns[total_columns.index(current_column) + 1]
        self.setText(9, "条数:{}/{}".format(len(data_total) + 1, len(dfs[0])) + "\n" + "列名顺序:{}/{}".format(
            total_columns.index(current_column), len(total_columns)) + "\n" + "列名:{}".format(current_column))

        #   更新其它九个方框的内容
        for i in range(8):
            if current_column in dfs[i].columns:
                data_temp = str(dfs[i].loc[len(data_total), current_column])
                if data_temp == 'nan':
                    self.setText(i, "")
                else:
                    self.setText(i, data_temp)
            else:
                self.setText(i, "")
        #   观测是否录入过,以选择是否更改显示信息.
        if current_column in data_single:
            self.setText(8, data_single[current_column])
        else:
            self.setText(8, "")

    # "上一条"按钮点击后执行的操作.注,上一条不得跨数据.
    def button10_action(self):
        global data_single, current_column, total_columns, data_total, output_file
        # 如果data_single不为空,说明不是新的一行数据.这时将光标current_column前移,更新数据
        if len(data_single) > 0:
            current_column = total_columns[total_columns.index(current_column) - 1]
        # 如果data_single为空,说明这是新的数据,需要导出上一条数据然后删除最后一条.同时输出一次文件
        else:
            data_single, data_total = data_total[-1], data_total[:-1]
            print("考察中数据:{}".format(data_single))
            print("考察中数据字段名:{}".format(data_single.keys()))
            print("待删除字段名:{}".format(list(data_single.keys())[-1]))
            del data_single[list(data_single.keys())[-1]]
            pd.DataFrame(data_total).to_csv(output_file, encoding='utf-8', index=False)
            current_column = total_columns[-1]
            print("当前字段:{}".format(current_column))
        self.setText(9, "条数:{}/{}".format(len(data_total) + 1, len(dfs[0])) + "\n" + "列名顺序:{}/{}".format(
            total_columns.index(current_column), len(total_columns)) + "\n" + "列名:{}".format(current_column))

        #   更新其它九个方框的内容
        for i in range(8):
            if current_column in dfs[i].columns:
                data_temp = str(dfs[i].loc[len(data_total), current_column])
                if data_temp == 'nan':
                    self.setText(i, "")
                else:
                    self.setText(i, data_temp)
            else:
                self.setText(i, "")
        #   观测是否录入过,以选择是否更改显示信息.
        if current_column in data_single:
            self.setText(8, data_single[current_column])
        else:
            self.setText(8, "")

    #   根据编号修改对应内容
    def setText(self, index, content):
        dict_ = {0: "迪斯科文字框", 1: "荷兰文字框", 2: "世界猫文字框", 3: "迪斯科_谷歌文字框", 4: "荷兰_谷歌文字框", 5: "世界猫_谷歌文字框", 6: "亚马逊文字框",
                 7: "亚马逊_谷歌文字框", 8: "目标字段文字框", 9: "字段信息文字框"}
        edittext = self.findChild(QTextEdit, dict_[index])
        edittext.setText(content)


if __name__ == '__main__':
    import sys

    # 新建app
    app = QApplication(sys.argv)

    # 初始化窗口window
    window = Window()

    # 显示窗口
    window.show()

    # 开始运行消息队列
    sys.exit(app.exec_())
