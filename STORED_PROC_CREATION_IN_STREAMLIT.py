# STREAMLIT SCRIPT TO BE PUT IN GITHUB WHICH NEED TO BE LINKED TO STREAMLIT ACCOUNT (this will be on free-version of streamlit account)

import streamlit as st
import re
import os


# Set up the Streamlit app
with st.sidebar:
    st.title('Snowflake SQL - Of Stored Procedure & Task Creation For Automation')
    st.header("Note that everytime after you have made a change, kindly press 'Enter' and review the scripts generated from preview section.")
    st.subheader("The conversion will take care of line breaks, comments and double-quote symbols by itself.")


## Snowflake SQL Scripts to Stored Procedures: 
text_input = st.text_area("Enter your SQL Script (Make sure that every SQL actions are separated by delimiter ';'):")


if text_input is not None:
    with open("sql_script.sql", "w") as f:
        f.write(text_input)

data_schema_location = st.text_input("For stored procedure to be created, kindly enter database name followed by schema name in such format: '[dbname].[schema]', i.e.: databaseA.schemaA ")
data_schema_location = data_schema_location.rstrip('.')
data_schema_location = data_schema_location + "."

def get_codes(f_name):
    with open(f_name, "r") as f:
        lines = f.readlines()
        for index, line in enumerate(lines):
            if (line.strip().startswith("--") ) or (line.strip().startswith("//") ) or (line == "\n"):
                lines[index] = ""
            else:
                lines[index] = line
    
    with open("new_"+f_name, "w") as f:
        for line in lines:
            f.write(line)         


def remv_cmmt_in_line(f_name):
    with open(f_name, "r") as f:
        lines = f.readlines()
        for index, line in enumerate(lines):
            if ("/*" in line) and ("*/" in line):
                lines[index] = re.sub(r'\/.*\/', '', line)
            else:
                lines[index] = line
                
    with open(f_name, "w") as f:
        for line in lines: 
            f.write(line) 

def remv_cmmt_eol(f_name):
    with open(f_name, "r") as f:
        lines = f.readlines()
        for index, line in enumerate(lines):
            if ("--" in line):
                DD_id = line.find("--")
                lines[index] = line[0:DD_id-1]+"\n"
            else:
                lines[index] = line
                
    with open(f_name, "w") as f:
        for line in lines:
            f.write(line)  

def remv_cmmt_ool(f_name):
    with open(f_name, "r") as f:
        lines = f.readlines()
        for index, line in enumerate(lines):
            if ("/*" in line):
                counter = index
                new_line = lines[counter]
                while  ("*/" not in new_line):
                    new_line = lines[counter].lstrip()+ lines[counter + 1]
                    counter +=1
                    lines[counter] = ""
                lines[index] = ""
            
            else:
                lines[index] = line
                
    with open(f_name, "w") as f:
        for line in lines:
            f.write(line)  

def script_beauty(f_name):
    get_codes(f_name)
    remv_cmmt_in_line("new_"+f_name)
    remv_cmmt_ool("new_"+f_name)
    remv_cmmt_eol("new_"+f_name)
    

def create_script(f_name, sql_file_out , PROC_NAME = "TEST_SP"):
    new_line = open(f_name).read().strip().replace('\n', " ")
    new_line = re.compile(r"\s+").sub(" ", new_line)
    rexp=re.compile("(.*?);")
    new_line_split = rexp.findall(new_line)
    
    # sp_ = "SET "+PROC_NAME+" = $DB_TRG_SCHEMA||'."+PROC_NAME+"'; \n"+
    sp_ = "CREATE OR REPLACE PROCEDURE "+ data_schema_location +PROC_NAME+"()  \n"+"RETURNS VARCHAR NOT NULL \n"+"LANGUAGE JAVASCRIPT \n"+"EXECUTE AS CALLER \n"+"AS \n"+"$$ \nvar result = \"\"; \ntry{\n"
    
    for i in new_line_split:
        if "'" in i: 
            i = i.replace("'",'\'')
        if '"' in i:
            i = i.replace('"','\\"')
        elif '\\' in i: 
            i = i.replace('\\','\\\\')
        var_query = 'var tbl_query = "'+ i.strip() +';";\nvar stmt = snowflake.createStatement({sqlText:tbl_query});\nvar _resultSet = stmt.execute();\n\n\n'
        sp_ += var_query
    
    sp_end = 'return "Done!" \n } \n catch (err)  { \n result =  "Failed: Code: " + err.code + "\\n  State: " + err.state; \n result += "\\n  Message: " + err.message; \n result += "\\nStack Trace:\\n" + err.stackTraceTxt; \n result += "\\n"+tbl_query;} \n return result; \n $$ \n;\n\n\n\n'
    new_codes = sp_ + sp_end + task_script
    with open(sql_file_out, "w") as f:
        for line in new_codes:
            f.write(line)  


def line_prepender(f_name, line):
    with open(f_name, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)

   

#insert name for stored procedure
name_proc = st.text_input("What you want your stored procedure called?")


# Create task
task_name = st.text_input("If you want to create task from this stored procedure, enter a task name:")

if task_name != "":

    task_location = st.text_input("For task to be created, kindly enter database name followed by schema name in such format: '[dbname].[schema]', i.e.: databaseA.schemaA:")
    task_location = task_location.rstrip('.')
    task_location = task_location + "."

    warehouse = st.text_input("Enter the warehouse name to run the script:")


    cron_time = st.text_input("When you want your script to run? Kindly refer to [link](https://crontab.guru/) for reference to enter the time.")
    cron_time = cron_time.strip()

    cron_region = st.text_input("Time zone for the script to execute? Example: UTC; Asia/Kuala_Lumpur.")
    cron_region = cron_region.strip()

    def create_task(task_name, cron_time, cron_region):

        sp_cont = "CREATE OR REPLACE TASK " + task_location +task_name + "\n" \
        + "WAREHOUSE= " + warehouse + "\n" \
        + "SCHEDULE = 'USING CRON " + cron_time + " " + cron_region + "'\n" \
        + "USER_TASK_TIMEOUT_MS = 86400000 AS CALL " + data_schema_location + name_proc + "();" + "\n"+ "\n" \
        + "ALTER TASK "+task_location +task_name + " RESUME;"
        

        return sp_cont

    task_script = create_task(task_name, cron_time, cron_region)

else: 

    task_script = ""        

def stored_proc_creation(f_name, name_proc):
    script_beauty(f_name)       
    create_script("new_"+f_name, PROC_NAME = name_proc, sql_file_out = 'SP_SQL.sql')
    os.remove("new_"+f_name)



if name_proc is not None and  text_input != "":
    stored_proc_creation('sql_script.sql', name_proc)
    

    with open('SP_SQL.sql', 'r') as f:
        lines = f.read()

    # Create a button that the user can click
    button = st.button("Preview of the full SQL")

    # Create an empty space in the app
    st.empty()

    # If the button has been clicked, display the text box
    if button:
        st.text(lines)

    st.download_button('Download full .sql file', lines, name_proc + ".sql" )
