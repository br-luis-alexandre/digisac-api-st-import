#pip install PyQt5
#pip install fbs
#fbs startproject
'''
import sys
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout, QPlainTextEdit
from PyQt5.QtGui import QIcon
from PyQt5.QtCore import pyqtSlot, QProcess
'''

# importing libraries - import all the required modules  
from PyQt5.QtWidgets import *
from PyQt5 import QtCore, QtGui
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import sys

import mainimportusers as usuarios
import mainimportdepartments as departamentos
import mainimportcontacts as contatos
import mainimportmessages as mensagens

class MainWindow(QWidget):
    count = 0

    def __init__(self, parent = None):
        super(MainWindow, self).__init__(parent)
        self.initUI()

    def initUI(self):
        
        # set the window title  
        self.setWindowTitle("[ST] Importador DIGISAC v.1.0")

        # set the window geometry 
        self.setGeometry(50, 50, 50, 50)


        self.setFixedSize(400, 300)
        self.resize(640, 480)


        #creating a button to be clicked
        self.bnt_Imp_Users = QPushButton('Importar Digisac.Users', self)
        self.bnt_Imp_Departments = QPushButton('Importar Digisac.Departments', self)
        self.bnt_Imp_Contacts = QPushButton('Importar Digisac.Contacts', self)
        self.bnt_Imp_Messages = QPushButton('Importar Digisac.Messages', self)
        #creating a textarea 
        self.txtarea_Result = QPlainTextEdit(self)    

        self.layout = QVBoxLayout(self) 
        self.layout.addWidget(self.bnt_Imp_Users)
        self.layout.addWidget(self.bnt_Imp_Departments)
        self.layout.addWidget(self.bnt_Imp_Contacts)
        self.layout.addWidget(self.bnt_Imp_Messages)  
        self.layout.addWidget(self.txtarea_Result)     

        self.bnt_Imp_Users.clicked.connect(self.on_click_bnt_Imp_Users) 
        self.bnt_Imp_Departments.clicked.connect(self.on_click_bnt_Imp_Departments)
        self.bnt_Imp_Contacts.clicked.connect(self.on_click_bnt_Imp_Contacts)
        self.bnt_Imp_Messages.clicked.connect(self.on_click_bnt_Imp_Messages) 


        # Create a label for displaying the time
        self.time_label = QLabel(self)
        self.layout.addWidget(self.time_label)
        # Create a QTimer that updates the time label every second
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)  # Update every 1000 milliseconds (1 second)

    @pyqtSlot()
    def update_time(self):
        # Get the current time and set it on the label
        current_time = QTime.currentTime().toString("hh:mm:ss")
        self.time_label.setText(f"Current Time: {current_time}")

    @pyqtSlot()
    def on_click_bnt_Imp_Users(self):
        self.txtarea_Result.insertPlainText("Início Importação Usuários...\n")
        usuarios.processar_paginas()
        self.txtarea_Result.insertPlainText("Fim Importação Usuários...\n")
        print("Fim Importação Usuários...\n")


    @pyqtSlot()
    def on_click_bnt_Imp_Departments(self):
        self.bnt_Imp_Departments.setEnabled(False)
        self.txtarea_Result.insertPlainText("Início Importação Departamentos...\n")
        departamentos.processar_paginas()
        self.txtarea_Result.insertPlainText("Fim Importação Departamentos...\n")
        self.bnt_Imp_Departments.setEnabled(True)

    @pyqtSlot()
    def on_click_bnt_Imp_Contacts(self):
        self.bnt_Imp_Departments.setEnabled(False)
        self.txtarea_Result.insertPlainText("Início Importação Contatos...\n")
        contatos.processar_paginas()
        self.txtarea_Result.insertPlainText("Fim Importação Contatos...\n")
        self.bnt_Imp_Departments.setEnabled(True)

    @pyqtSlot()
    def on_click_bnt_Imp_Messages(self):
        self.bnt_Imp_Departments.setEnabled(False)
        self.txtarea_Result.insertPlainText("Início Importação Mensagens...\n")
        mensagens.processar_paginas()
        self.txtarea_Result.insertPlainText("Fim Importação Mensagens...\n")
        self.bnt_Imp_Departments.setEnabled(True)


def closeEvent():
        #Your desired functionality here
        print('Close button pressed')
        import sys
        sys.exit(0)


def main():
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)  # Make the UI responsive
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(closeEvent)
    ex = MainWindow()
    ex.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()