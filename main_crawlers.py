from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QComboBox, QLabel, QTextEdit, QFileDialog, QMessageBox
from os.path import isfile
from math import floor
from crawlers import crawlers_tools
from crawlers import pymarc_tools
from crawlers import globalValues

TARGETS = ["无", "CD-亚马逊", "CD-迪斯科", "CD-荷兰", "CD-世界猫", "CD-谷歌", "韩文抓取", "CD-csv->iso", "韩文iso文件规范",
           "书-伯克利", "书-耶鲁", "书-英图", "书-密歇根", "书-美图", "iso空白统计", "书-五合一"]


class Window(QWidget):

    # 初始化窗口,设置窗口的尺寸与位置。默认设置为3000*1800/1500*800的尺寸。同时设置四个区域(1列表区,2预览区,3流程区,4按钮区)
    def __init__(self):
        super().__init__()
        self.setWindowTitle("书目打包软件")
        self.setWindowTitle("书目打包软件")

        # 调整大小
        self.resize_x, self.resize_y = 1500, 1050
        self.resize(self.resize_x, self.resize_y)

        # 调整初始显示位置
        self.move(0, 50)

        # 初始化步骤列表
        self.mission = []

        # 初始化选区
        self.widget1 = self.add_widget('white', self.resize_x / 12, self.resize_y * 2 / 3, 0, 0)
        self.widget2 = self.add_widget('white', self.resize_x * 4 / 6, self.resize_y, self.resize_x / 12, 0)
        self.widget3 = self.add_widget('white', self.resize_x * 3 / 12, self.resize_y, self.resize_x * 9 / 12, 0)
        self.widget4 = self.add_widget('white', self.resize_x / 12, self.resize_y / 3, 0, self.resize_y * 2 / 3)

        # 初始化列表区,1区
        self.init_widget1()

        # 初始化预览区,2区
        self.init_widget2()

        # 初始化流程区,3区
        self.init_widget3()

        # 初始化按钮区,4区
        self.init_widget4()

    # 初始化列表区,1区
    def init_widget1(self):
        # 标志为"请选择脚本:"的辅助提示
        self.add_label(self.widget1, '选项:', self.widget1.resize_x * 2 / 3, self.widget1.resize_y * 3 / 60,
                       self.widget1.resize_x * 8 / 30, self.widget1.resize_y / 60)
        # 标志为"脚本清单"的下拉选项框
        self.add_menu(self.widget1, TARGETS,
                      self.widget1.resize_x * 2 / 3,
                      self.widget1.resize_y / 20,
                      self.widget1.resize_x / 3, self.widget1.resize_y / 12)

    # 初始化预览区,2区
    def init_widget2(self):
        globalValues._init()
        # 标志为"内容区"的辅助提示
        self.add_label(self.widget2, "内容区", self.widget2.resize_x * 2 / 3, self.widget2.resize_y / 20,
                       self.widget2.resize_x * 1 / 16, self.widget2.resize_y * 1 / 60)
        # 标志为"内容区文字框"的文字框
        self.add_text(self.widget2, '内容区文字框', self.widget2.resize_x * 7 / 8, self.widget2.resize_y * 15 / 60,
                      self.widget2.resize_x * 1 / 16, self.widget2.resize_y * 4 / 60)
        edittext = self.findChild(QTextEdit, "内容区文字框")
        globalValues.set_value("内容区文字框", edittext)
        # 标志为"注意区"的辅助提示
        self.add_label(self.widget2, "注意区", self.widget2.resize_x * 2 / 3, self.widget2.resize_y / 20,
                       self.widget2.resize_x * 1 / 16, self.widget2.resize_y * 20 / 60)
        # 标志为"注意区文字框"的文字框
        self.add_text(self.widget2, '注意区文字框', self.widget2.resize_x * 7 / 8, self.widget2.resize_y * 15 / 60,
                      self.widget2.resize_x * 1 / 16, self.widget2.resize_y * 23 / 60)
        # 标志为"显示区"的辅助提示
        self.add_label(self.widget2, "显示区", self.widget2.resize_x * 2 / 3, self.widget2.resize_y / 20,
                       self.widget2.resize_x * 1 / 16, self.widget2.resize_y * 39 / 60)
        # 标志为"显示区文字框"的文字框
        self.add_text(self.widget2, '显示区文字框', self.widget2.resize_x * 7 / 8, self.widget2.resize_y * 15 / 60,
                      self.widget2.resize_x * 1 / 16, self.widget2.resize_y * 42 / 60)

    # 初始化流程区,3区
    def init_widget3(self):
        # 标志为"流程区"的辅助提示
        self.add_label(self.widget3, "流程区", self.widget3.resize_x * 4 / 5, self.widget3.resize_y / 20,
                       self.widget3.resize_x * 1 / 10, 0)
        # 标志为"流程区文字框"的文字框
        self.add_text(self.widget3, '流程区文字框', self.widget3.resize_x * 4 / 5, self.widget3.resize_y * 9 / 10,
                      self.widget3.resize_x * 1 / 10, self.widget2.resize_y / 20)

    # 初始化按钮区,4区
    def init_widget4(self):
        button1 = self.add_button(self.widget4, '查看功能', 100, 30,
                                  (self.widget4.resize_x - 100) / 2, self.widget4.resize_y / 6 - 15)
        button2 = self.add_button(self.widget4, '加入步骤', 100, 30,
                                  (self.widget4.resize_x - 100) / 2, self.widget4.resize_y * 2 / 6 - 15)
        button3 = self.add_button(self.widget4, '删除步骤', 100, 30,
                                  (self.widget4.resize_x - 100) / 2, self.widget4.resize_y * 3 / 6 - 15)
        button4 = self.add_button(self.widget4, '执行任务', 100, 30,
                                  (self.widget4.resize_x - 100) / 2, self.widget4.resize_y * 4 / 6 - 15)
        button5 = self.add_button(self.widget4, '日志记录', 100, 30,
                                  (self.widget4.resize_x - 100) / 2, self.widget4.resize_y * 5 / 6 - 15)
        button1.clicked.connect(self.button1_action)
        button2.clicked.connect(self.button2_action)
        button3.clicked.connect(self.button3_action)
        button4.clicked.connect(self.button4_action)

    # 增添区域
    def add_widget(self, color, resize_x, resize_y, move_x, move_y):
        widget = QWidget(self)
        widget.setStyleSheet("background-color: {};".format(color))
        widget.resize(floor(resize_x), floor(resize_y))
        widget.resize_x, widget.resize_y, widget.move_x, widget.move_y = floor(resize_x), floor(resize_y), floor(
            move_x), floor(move_y)
        widget.move(floor(move_x), floor(move_y))
        return widget

    # 增添单行文字提示
    def add_label(self, widget, text, resize_x, resize_y, move_x, move_y):
        label = QLabel(widget)
        label.setObjectName(text)
        label.setText(text)
        label.resize(floor(resize_x), floor(resize_y))
        label.move(floor(move_x), floor(move_y))

    # 增添多行文字提示
    def add_text(self, widget, text, resize_x, resize_y, move_x, move_y):
        text_edit = QTextEdit(widget)
        text_edit.setObjectName(text)
        text_edit.setText(text)
        text_edit.resize(floor(resize_x), floor(resize_y))
        text_edit.move(floor(move_x), floor(move_y))

    # 增添下拉列表按钮
    def add_menu(self, widget, items, resize_x, resize_y, move_x, move_y):
        combobox = QComboBox(widget)
        # 设定id
        combobox.setObjectName("脚本清单")
        # 设定大小
        combobox.resize(floor(resize_x), floor(resize_y))
        # 选定位置
        combobox.move(floor(move_x), floor(move_y))
        # 增加选项
        combobox.addItems(items)

    # 在区域中增添点击按钮,并设置尺寸与摆放位置
    def add_button(self, widget, title, resize_x, resize_y, move_x, move_y):
        button_ = QPushButton(widget)
        # button_.setParent(widget)
        button_.setText(title)
        button_.setObjectName(title)
        button_.resize(resize_x, resize_y)
        button_.move(floor(move_x), floor(move_y))
        return button_

    # 删除点击按钮
    def delete_button(self, title):
        self.buttons[title].close()
        del self.buttons[title]

    # "查看功能"按钮点击后执行的操作
    def button1_action(self, name):
        combobox = self.findChild(QComboBox, "脚本清单")
        edittext = self.findChild(QTextEdit, "内容区文字框")
        edittext2 = self.findChild(QTextEdit, "注意区文字框")
        index = combobox.currentIndex()
        if isfile("_Mission{}.txt".format(index)):
            fp_ = open("_Mission{}.txt".format(index), 'r', encoding='utf-8')
            strs_, str_ = '', fp_.readline()
            while str_:
                strs_ += str_
                str_ = fp_.readline()
            edittext.setText(str(strs_))
        else:
            edittext.setText("当前任务的说明书缺失,请联系管理员！")

        if isfile("_Mission{}_warning.txt".format(index)):
            fp_ = open("_Mission{}_warning.txt".format(index), 'r', encoding='utf-8')
            strs_, str_ = '', fp_.readline()
            while str_:
                strs_ += str_
                str_ = fp_.readline()
            edittext2.setText(str(strs_))
        else:
            edittext2.setText("当前注意事项尚未更新,请联系管理员！")

    # "加入步骤"按钮点击后执行的操作
    def button2_action(self):
        combobox = self.findChild(QComboBox, "脚本清单")
        edittext = self.findChild(QTextEdit, "流程区文字框")
        index = combobox.currentIndex()
        # 先将当前任务添加入列表
        if index > 0:
            self.mission.append(TARGETS[index])
        # 将列表在流程区中输出
        strs_ = ''
        for index, str_ in enumerate(self.mission):
            strs_ += "第{}步执行: ".format(index + 1) + str_ + "\n"
        edittext.setText(strs_)

    # "删除步骤"按钮点击后执行的操作
    def button3_action(self):
        edittext = self.findChild(QTextEdit, "流程区文字框")
        # 先删除最终任务
        self.mission.pop()
        # 再输出当前列表
        strs_ = ''
        for index, str_ in enumerate(self.mission):
            strs_ += "第{}步执行: ".format(index + 1) + str(str_) + "\n"
        edittext.setText(strs_)

    # "执行任务"按钮点击后执行的操作
    def button4_action(self):
        edittext = self.findChild(QTextEdit, "内容区文字框")
        edittext.setText("")
        text = ''
        if len(self.mission) > 0:
            # 依序执行
            for index, mission in enumerate(self.mission):
                # 先根据类别弹出文件夹对话框or文件对话框
                # csv文件部分
                if mission in ["CD-亚马逊", "CD-迪斯科", "CD-荷兰", "CD-世界猫", "CD-谷歌", "CD-csv->iso"]:
                    directory = QFileDialog.getOpenFileName(self, "请选取待转化的csv文件", "./", "*.csv")[0][:-4]
                    if mission == "CD-亚马逊":
                        crawlers_tools.crawler_for_cd(directory + ".csv", directory + "_亚马逊.csv", 2, "Amazon")
                        edittext.setText("亚马逊抓取完成.")
                    elif mission == "CD-迪斯科":
                        crawlers_tools.crawler_for_cd(directory + ".csv", directory + "_迪斯科.csv", 2, "Disco")
                        edittext.setText("迪斯科抓取完成.")
                    elif mission == "CD-荷兰":
                        crawlers_tools.crawler_for_cd(directory + ".csv", directory + "_荷兰.csv", 1,
                                                      "Neitherlands")
                        edittext.setText("荷兰抓取完成.")
                    elif mission == "CD-世界猫":
                        crawlers_tools.crawler_for_cd(directory + ".csv", directory + "_世界猫.csv", 2, "Worldcat")
                        edittext.setText("世界猫抓取完成.")
                    elif mission == "CD-谷歌":
                        crawlers_tools.crawler_for_cd(directory + ".csv", directory + "_谷歌.csv", 2, "Google")
                        edittext.setText("谷歌抓取完成.")
                    elif mission == "CD-csv->iso":
                        pymarc_tools.output_iso(directory + ".csv")
                        edittext.setText("csv文件转换iso文件完成.")
                # txt文件部分
                elif mission in ["韩文抓取", "书-伯克利", "书-耶鲁", "书-英图", "书-密歇根", "书-美图"]:
                    directory = QFileDialog.getOpenFileName(self, "请选取待转化的txt文件", "./", "*.txt")[0][:-4]
                    if mission == "韩文抓取":
                        crawlers_tools.crawler_for_cd(directory + ".txt", directory + "_韩文抓取.csv", 2, "South Korea",
                                                      False)
                        edittext.setText("韩文抓取完成.")
                    elif mission == "书-伯克利":
                        crawlers_tools.crawler_for_cd(directory + ".txt", directory + "_Berkeley.iso", 2, "Berkeley",
                                                      False)
                        edittext.setText("Berkeley抓取完成.")
                    elif mission == "书-耶鲁":
                        crawlers_tools.crawler_for_cd(directory + ".txt", directory + "_Yale.iso", 2, "Yale",
                                                      False)
                        edittext.setText("Yale抓取完成.")
                    elif mission == "书-英图":
                        crawlers_tools.crawler_for_cd(directory + ".txt", directory + "_British.iso", 2, "British",
                                                      False)
                        edittext.setText("British抓取完成.")
                    elif mission == "书-密歇根":
                        crawlers_tools.crawler_for_cd(directory + ".txt", directory + "_Michigan.iso", 2, "Michigan",
                                                      False)
                        edittext.setText("Michigan抓取完成.")
                    elif mission == "书-美图":
                        crawlers_tools.crawler_for_cd(directory + ".txt", directory + "_US.iso", 2, "US",
                                                      False)
                        edittext.setText("US抓取完成.")
                # iso文件部分
                elif mission in ["韩文iso文件规范"]:
                    directory = QFileDialog.getOpenFileName(self, "请选取待转化的iso文件", "./", "*.iso")[0][:-4]
                    if mission == "韩文iso文件规范":
                        pymarc_tools.South_Korea_format(directory + ".iso", directory + "_软件生成.iso")
                        edittext.setText("韩文iso文件规范完成.")
                # 文件夹部分
                elif mission in ["书-五合一"]:
                    directory = QFileDialog.getExistingDirectory(self, "请选取待批量处理的文件夹", "./")
                    if mission == "书-五合一":
                        pymarc_tools.merge_five_isos(directory)
                        edittext.setText("五合一完成.")
                # iso文件+txt文件
                elif mission in ["iso空白统计"]:
                    directory1 = QFileDialog.getOpenFileName(self, "请选取待转化的iso文件(待转化iso)", "./", "*.iso")[0][:-4]
                    directory2 = QFileDialog.getOpenFileName(self, "请选取待转化的txt文件(原始书号)", "./", "*.txt")[0][:-4]
                    if mission == "iso空白统计":
                        pymarc_tools.get_blanks_from_iso(directory1 + ".iso", directory2 + ".txt")
                        edittext.setText("iso空白统计完成.")


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
