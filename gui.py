from tkinter import _setit
from tkinter import messagebox
from pandas_helper import *
from tkinter import *
from tkinter.scrolledtext import ScrolledText
import os
from ruamel.yaml import YAML
import subprocess


def database_connectors():
    if os.path.isfile('../databases.yml'):
        path = '../databases.yml'
    else:
        path = 'databases.yml'
    return dict(YAML().load(open(path, 'r')))


def get_folder_from_connector(conn):
    return '_'.join(conn.split('_')[:-1]).upper()


def get_schema_tables_dict(conn_var):
    folder = get_folder_from_connector(conn_var)
    test_config_file = os.path.dirname(os.path.abspath(__file__)) + '/' + folder + '/test_' + folder.lower()
    return read_data(test_config_file)['schema_tables_dict']


def get_query_pairs_from_source_folder(conn_var):
    folder = get_folder_from_connector(conn_var)
    test_config_file = os.path.dirname(os.path.abspath(__file__)) + '/' + folder + '/test_edw'
    if os.path.isfile(test_config_file + '.yml'):
        if os.stat(test_config_file + '.yml').st_size > 0:
            return filter_dicts(read_data(test_config_file), ['query_pair'])
        else:
            return {}
    else:
        open(test_config_file + '.yml', 'a')
        return {}


def get_query_pairs_from_edw_folder():
    folder = 'EDW'
    test_config_file = os.path.dirname(os.path.abspath(__file__)) + '/' + folder + '/test_' + folder.lower()
    return filter_dicts(read_data(test_config_file), ['query_pair'])


def on_stop_press():
    cmd = "taskkill /F /im pytest.exe /S " + os.environ['COMPUTERNAME'] + " /U " + os.getlogin()
    print(cmd)
    os.system(cmd)


def open_current_log():
    subprocess.Popen("\"C:\\Program Files (x86)\\Notepad++\\notepad++.exe\"  c:\\pytest\\outputfile.log")


class MyWindow:

    def __init__(self, win):

        # Run button
        self.b1 = Button(win, text='Run Tables', command=self.run)
        self.b1.place(x=250, y=650)

        # Run button2 - RUN EDW Queries
        self.b2 = Button(win, text='Run Queries', command=self.run2)
        self.b2.place(x=800, y=650)

        # Run button4 - SAVE EDW Queries
        self.b4 = Button(win, text='Save Query Pair', command=self.save_queries)
        self.b4.place(x=900, y=50)

        # Stop Run button3
        self.b3 = Button(win, text='Stop Run', command=on_stop_press)
        self.b3.place(x=500, y=650)

        # Button5 - Open results folder
        # self.b3 = Button(win, text='Stop Run', command=on_stop_press)
        self.b5 = Button(win, text="Open Results", command=self.open_results_folder)
        self.b5.place(x=950, y=650)

        # Button6 - Open Log file
        self.b6 = Button(win, text="Open Current Log", command=open_current_log)
        self.b6.place(x=1100, y=650)

        # Source Type
        self.conn_var1 = StringVar(win)
        self.conn_var1.set("CAPS_prod")
        self.op1_lbl = Label(win, text='Source Type')
        filtered_source_list = filter_dicts(database_connectors(), ['caps', 'light', 'edge', 'sequel']).keys()
        self.op1 = OptionMenu(win, self.conn_var1, *list(filtered_source_list), command=self.on_source_type_change)
        self.op1.pack()
        self.op1_lbl.place(x=25, y=25)
        self.op1.place(x=25, y=50)

        # DropDown Option Menu - Source connector 1
        self.conn_var2 = StringVar(win)
        self.conn_var2.set("data_lake_prod")
        self.conn_op2_lbl = Label(win, text='Source')
        filtered_target_list = filter_dicts(database_connectors(),
                                            ['caps', 'light', 'edge', 'sequel', 'data', 'snow']).keys()
        self.conn_op2 = OptionMenu(win, self.conn_var2, *list(filtered_target_list))
        self.conn_op2.pack()
        self.conn_op2_lbl.place(x=200, y=25)
        self.conn_op2.place(x=200, y=50)

        # Target connector
        self.conn_var3 = StringVar(win)
        self.conn_var3.set("edw_dev")
        self.conn_op3_lbl = Label(win, text='Target')
        filtered_target_list = filter_dicts(database_connectors(), ['data', 'snow', 'edw']).keys()
        self.conn_op3 = OptionMenu(win, self.conn_var3, *list(filtered_target_list))
        self.conn_op3.pack()
        self.conn_op3_lbl.place(x=350, y=25)
        self.conn_op3.place(x=350, y=50)

        # Query Pair selector
        self.conn_var4 = StringVar(win)
        folder = self.conn_var1.get().split('_')[0]
        self.conn_var4.set(folder + " Query Pairs")
        self.conn_op4_lbl = Label(win, text='Saved ' + folder + ' Query Pairs')
        filtered_query_pair_list = get_query_pairs_from_source_folder(self.conn_var1.get()).keys()
        self.conn_op4 = OptionMenu(win, self.conn_var4, *list(filtered_query_pair_list),
                                   command=self.on_query_pair_change)
        self.conn_op4.pack()
        self.conn_op4_lbl.place(x=550, y=25)
        self.conn_op4.place(x=550, y=50)

        # Query1 Text Box
        self.query1_lbl = Label(win, text='Source Query')
        self.query1 = ScrolledText(win, width=40, height=30)
        self.query1.insert('1.0',
                           "select distinct CONCAT(TRIM(a00_pnum), '|', a06_edition, '|', e01_transnum, '|', t91_pcov_key) as POLICY_COVERAGE from caps_cmsi.pcoverage")
        self.query1.pack()
        self.query1_lbl.place(x=550, y=100)
        self.query1.place(x=550, y=125)
        # Query2 Text Box
        self.query2_lbl = Label(win, text='Target Query')
        self.query2 = ScrolledText(win, width=40, height=30)
        self.query2.insert('1.0',
                           "select SOURCE_SYSTEM_KEY from EDW_DEV.STAGING.POLICY_COVERAGE as POLICY_COVERAGE WHERE SOURCE_SYSTEM in ('CAPS')")
        self.query2.pack()
        self.query2_lbl.place(x=900, y=100)
        self.query2.place(x=900, y=125)

        # schemas List box
        schema_tables_dict = get_schema_tables_dict(self.conn_var1.get())
        self.schemas_var = StringVar(win)
        self.schemas_var.set(list(schema_tables_dict.keys()))
        self.schemas_lb_lbl = Label(win, text='Schemas')
        self.schemas_lb = Listbox(win, listvariable=self.schemas_var, selectmode="multiple", exportselection=False)
        self.schemas_lb.bind("<<ListboxSelect>>", self.on_schema_select)
        self.schemas_lb.pack()
        self.schemas_lb_lbl.place(x=25, y=100)
        self.schemas_lb.place(x=25, y=125)

        # self.frame = Frame(win)
        # self.scrollbar = Scrollbar(win, orient=VERTICAL)
        # yscrollcommand = self.scrollbar.set,
        # self.tables_lb.pack(side="left", fill="y")
        # self.tables_lb = Listbox(win, )
        # self.scrollbar.config(command=self.tables_lb.yview)
        # self.scrollbar.pack(side=RIGHT, fill=Y)
        # self.tables_lb.pack(side=LEFT, fill=BOTH, expand=1)
        self.tables_var = StringVar(win)
        # self.tables_var.set([elem[0] for elem in schema_tables_dict.values()])
        self.tables_lb_lbl = Label(win, text='Tables')
        self.tables_lb = Listbox(win, listvariable=self.tables_var, width=40, height=20,
                                 selectmode="multiple", exportselection=False)
        self.tables_lb.pack()
        self.tables_lb_lbl.place(x=200, y=100)
        self.tables_lb.place(x=200, y=125)

    def open_results_folder(self):
        folder = self.conn_var1.get().split('_')[0]
        os.system("start C:/pytest/"+folder.upper()+"/output")

    def on_source_type_change(self, event):
        # update schemas list
        schema_tables_dict = get_schema_tables_dict(event)
        self.schemas_var.set(list(schema_tables_dict.keys()))
        self.schemas_lb.delete(0, END)
        for item in list(schema_tables_dict.keys()):
            self.schemas_lb.insert(END, item)
            print(item)

        # update query pairs options
        folder = get_folder_from_connector(self.conn_var1.get())
        self.conn_op4_lbl['text'] = 'Saved ' + folder + ' Query Pairs'
        # test_config_file = os.path.dirname(os.path.abspath(__file__)) + '/' + folder + '/test_edw'
        filtered_query_pair_list = get_query_pairs_from_source_folder(self.conn_var1.get()).keys()
        menu = self.conn_op4["menu"]
        if len(filtered_query_pair_list) > 0:
            # Reset var and delete all old options
            self.conn_var4.set('')
            menu.delete(0, "end")
            for option in filtered_query_pair_list:
                menu.add_command(label=option, command=_setit(self.conn_var4, option, self.on_query_pair_change))
            self.conn_var4.set(folder + " Query Pairs")
            # self.conn_op4 = OptionMenu(win, self.conn_var4, *list(filtered_query_pair_list),
            #                            command=self.on_query_pair_change)
        else:
            menu.delete(0, "end")
            self.conn_var4.set("No queries saved for " + get_folder_from_connector(self.conn_var1.get()))

    def save_queries(self):
        existing_query_pairs = get_query_pairs_from_source_folder(self.conn_var1.get())
        existing_queries_len = len(existing_query_pairs)
        q1 = self.query1.get('1.0', END).strip()
        q2 = self.query2.get('1.0', END).strip()

        new_pair = {'query1': q1, 'query2': q2}
        if new_pair not in existing_query_pairs.values():
            folder = get_folder_from_connector(self.conn_var1.get())
            test_config_file = os.path.dirname(os.path.abspath(__file__)) + '/' + folder + '/test_edw'
            key = 'query_pair' + str(existing_queries_len + 1)
            append_data(test_config_file, {key: {'query1': q1, 'query2': q2}})
            menu = self.conn_op4["menu"]
            menu.add_command(label=key, command=_setit(self.conn_var4, key, self.on_query_pair_change))
            # self.conn_var4.set("Select one..")
            messagebox.showinfo("Information", "Saved " + key)
        else:
            messagebox.showerror("Error", "Query Pair already exists")

    def on_query_pair_change(self, event):
        query_pairs_dict = get_query_pairs_from_source_folder(self.conn_var1.get())
        self.query1.delete('1.0', END)
        self.query1.insert('1.0', query_pairs_dict[event]['query1'])

        self.query2.delete('1.0', END)
        self.query2.insert('1.0', query_pairs_dict[event]['query2'])

    def on_schema_select(self, event):
        schema_tables_dict = get_schema_tables_dict(self.conn_var1.get())
        widget = event.widget
        selection = widget.curselection()
        self.tables_lb.delete(0, END)
        if len(selection) != 0:
            self.tables_lb.delete(0, END)
            for idx in widget.curselection():
                schema = widget.get(idx)
                schema_tables = schema_tables_dict[schema]
                for table in list(schema_tables):
                    self.tables_lb.insert(END, schema + '__' + table)
                    # print(table)

    def run2(self):
        q1 = self.query1.get('1.0', END).strip()
        q2 = self.query2.get('1.0', END).strip()
        if 'snowflake' in self.conn_var3.get().lower() and 'data_lake' not in q2.lower():
            messagebox.showerror("Bad Query", "Snowflake query requires Database prefix before schema. please fix your query")
            return -1

        if '*' in q1 or '*' in q2:
            messagebox.showwarning("Warning", "Select * queries frequently run out of memory space.")
        if '*' in q1:
            # temp = "CREATE TEMPORARY TABLE query_out as " + q1
            # extract('data_lake_prod', temp)
            # s1 = dict(extract('data_lake_prod', "pg_total_relation_size('query_out')"))['pg_relation_size'][0] // 1024 // 1024
            # connection = get_connection('data_lake_prod')
            # pd.write_sql_query(temp, connection)
            # cur = connection.cursor()
            # cur.execute(temp)

            # temp = "CREATE TEMPORARY TABLE DATA_LAKE_DEV.ONEDATA_SYSTEM_STATUS.query_out as " + q1
            # t2 = "pg_total_relation_size('query_out')"
            # extract('data_lake_prod', temp)
            # # s1 = dict(extract('data_lake_prod', "pg_total_relation_size('query_out')"))['pg_relation_size'][0] // 1024 // 1024
            # connection = get_connection('data_lake_prod')
            # # connection = get_connection('snowflake')
            # temp = "CREATE TEMPORARY TABLE query_out as " + q1
            # connection.cursor().execute(temp)
            # # connection.execute(temp)

            schema_table_from_query1 = q1.upper().split('FROM ')[1].split(' ')[0]
            sch1 = schema_table_from_query1.split('.')[0]
            t1 = schema_table_from_query1.split('.')[1]
            testname = q1.upper().split('FROM ')[1].split(' ')[0].replace('"', '')
            s1 = size_of_table_in_MB(testname, sch1, t1)

            if s1 > 3000:
                messagebox.showerror("RESULT SIZE Error",
                                     "RESULT SIZE is " + str(s1) + " MB. This Test will run out of memory. "
                                     "Terminating execution. Please partition your query")
                return -1
            elif s1 > 1500:
                messagebox.showwarning("RESULT SIZE Warning",
                                       "RESULT SIZE is " + str(s1) + " MB. This Test will likely run out of memory.")

        conn2 = str(self.conn_var2.get())
        conn3 = str(self.conn_var3.get())
        folder = get_folder_from_connector(str(self.conn_var1.get()))
        cmd = "pytest ./" + folder + " -k columns --query1=\"" + q1 + "\" --query2=\"" + q2 + "\" --conn1=" + conn2 + " --conn2=" + conn3
        print(cmd)
        p = subprocess.Popen(cmd)
        # p2 = subprocess.Popen("\"C:\\Program Files (x86)\\Notepad++\\notepad++.exe\"  c:\\pytest\\outputfile.log")

    def run(self):
        conn1 = str(self.conn_var1.get())
        conn2 = str(self.conn_var2.get())
        conn3 = str(self.conn_var3.get())

        if 'edw' in conn3.lower():
            messagebox.showerror("Bad Connector", "Target EDW is not supported for select * queries."
                                 "Terminating execution. Please partition your query")
            return -1
        folder = get_folder_from_connector(conn1)

        # schema_tables_dict = get_schema_tables_dict(conn1)
        expression_filter = "\""
        widget = self.tables_lb
        if widget.curselection() != ():
            for idx in widget.curselection():
                schema_table = widget.get(idx)
                expression_filter = expression_filter + str(schema_table) + "__ or "
        else:
            widget2 = self.schemas_lb
            for idx in widget2.curselection():
                schema = widget2.get(idx)
                expression_filter = expression_filter + schema + "_ or "
        expression_filter = expression_filter[: -len(" or ")]
        cmd = "pytest ./" + folder + " --query1='' --query2='' --conn1=" + conn2 + " --conn2=" + conn3 + " -k " + expression_filter + "\""
        print(cmd)
        p = subprocess.Popen(cmd)


# mainloop()
window = Tk()
mywin = MyWindow(window)
window.title('Argo Content Validation runner')
window.geometry("1250x700+10+10")
window.mainloop()