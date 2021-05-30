from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
import os.path, time
import xlsxwriter
import datetime

path_dir = str(input("Enter the full path = "))

currdttm = str(datetime.date.today())
directories = []
faxwithfiles = []
maindata = []
maindataexcel = []


with open("Log_"+currdttm+".txt", "w") as file:

    for root, dirs, files in os.walk(path_dir):

        folder_date = path_dir[49:51]+"/"+path_dir[51:53]+"/"+path_dir[53:]
        file.write("Entered Folder Path : %s\n" %  path_dir)
        print("FAX Folder Date : ", folder_date)
        file.write("FAX Folder Date : %s\n" %  str(folder_date))
        directories.extend(dirs)

        if files == []:
            print("Faxes does not present in Folder : ", root)
            file.write("Faxes does not present in Folder : %s\n" % str(root))
        else:
            print("Yes!, Faxes presents in Folder : " + root + " | " + str(files))
            file.write("Yes!, Faxes presents in Folder : %s || %s\n" % (str(root),str(files)))
            faxwithfiles.append(root[49:] + " = " + str(len(files)) + " = " + str(files))

            for i in files:

                filefoldr = "%s/%s" % (root , i)
                faxname = i[:i.rfind(".")]
                print("FAX Name is : ", faxname)
                file.write("FAX Name is : %s\n" % str(faxname))
                fp = open(filefoldr, 'rb')
                fsize = os.path.getsize(filefoldr)
                fsizekb = round(float(fsize)/1024)
                print("FAX Size in KB : ", str(fsizekb) + " KB")
                file.write("FAX Size in KB : %s KB\n" % str(fsizekb))
                parser = PDFParser(fp)
                doc = PDFDocument(parser)
                moddate = time.strftime("%d.%m.%Y", time.gmtime(os.path.getmtime("{}".format(filefoldr))))
                print("FAX Last modified : %s" % moddate)
                file.write("FAX Last modified : %s\n" % str(moddate))
                folder_id = root[len(path_dir)+1:len(path_dir)+4]
                print("FAX Folder id : %s" % folder_id)
                file.write("FAX Folder id : %s\n" % str(folder_id))
                maindata.append(faxname)
                maindata.append(str(fsizekb) + " KB")
                maindata.append(moddate)
                maindata.append(folder_id)
                maindata.append(folder_date)
                maindataexcel.append(maindata)
                maindata = []

    print("\nProcess Completed !!")
    file.write("\nProcess Completed !!\n")

    print("\nTotal Directories inside given path : " + str(len(directories)) + " | " + str(directories))
    file.write("Total Directories inside given path : %s || %s\n" % (str(len(directories)),str(directories)))

    print("\nTotal Directorywise Faxes inside given path : " + str(faxwithfiles))
    file.write("Total Directorywise Faxes inside given path : %s\n" % str(faxwithfiles))

    print("\nTotal Records in Excel : ", len(maindataexcel) , " " , maindataexcel)
    file.write("Total Records in Excel : %s | %s\n" % (str(len(maindataexcel)),str(maindataexcel)))

with xlsxwriter.Workbook('FinalExcel.xlsx') as workbook:
    worksheet = workbook.add_worksheet()

    for row_num, data in enumerate(maindataexcel):
        worksheet.write_row(row_num, 0, data)

print("\nExcel created Successfully...")
# file.write("Excel created Successfully...")