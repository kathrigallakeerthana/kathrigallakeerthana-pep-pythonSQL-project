import csv
import sqlite3
import os

def return_cursor(conn):
    return conn.cursor()

def create_tables(conn):
    cursor=return_cursor(conn)
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')
    conn.commit()




# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path,conn):
    cursor=return_cursor(conn)
    with open(file_path,'r') as file:
        rd=csv.reader(file)
        next(rd)
        for i in rd:
            if len(i)==2 and all(field.strip() for field in i):
                cursor.execute("INSERT INTO  users(firstName,lastName) VALUES(?,?)",(i[0],i[1]))
    conn.commit()



# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path,conn):
    cursor=return_cursor(conn)
    with open(file_path,'r') as file:
        rd=csv.reader(file)
        next(rd)
        for i in rd:
            if len(i)==5 and all(i[:3]) and i[4].strip():
                try:
                    st=int(i[1])
                    et=int(i[2])
                    ud=int(i[4])
                    cursor.execute("""
                    INSERT INTO callLogs(phoneNumber,startTime,endTime,Direction,userId)
                    VALUES(?,?,?,?,?)""",
                    (i[0],st,et,i[3],ud))
                except ValueError:
                    continue
    conn.commit()


    


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_ordered_calls(file_path,conn):
    cursor=return_cursor(conn)
    cursor.execute("""
    SELECT callId,phoneNumber,startTime,endTime,Direction,userId
    from callLogs
    order by userId,startTime
    """)
    ol=cursor.fetchall()
    with open(file_path,'w',newline='') as file:
        wr=csv.writer(file)
        wr.writerow(['callId','phoneNumber','startTime','endTime','Direction','userId'])
        for i in ol:
            wr.writerow(i)

    
def write_user_analytics(file_path,conn):
    cursor=return_cursor(conn)
    cursor.execute("""
    select userId,
    AVG(endTime-startTime) as avgDuration,
    count(*) as numCalls
    from callLogs
    group by userId
    """)
    an=cursor.fetchall()
    with open(file_path,'w',newline='') as file:
        wr=csv.writer(file)
        wr.writerow(['userId','avgDuration','numCalls'])
        for i in an:
            wr.writerow([i[0],round(i[1],1),i[2]])

def main():
    conn=sqlite3.connect(':memory:')
    create_tables(conn)
    base_dir=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(file))))
    resources_dir=os.path.join(base_dir,'resources')
    users_file=os.path.join(resources_dir,'users.csv')
    call_logs_file=os.path.join(resources_dir,'callLogs.csv')
    user_analytics_file=os.path.join(resources_dir,'userAnalytics.csv')
    ordered_call_logs_file=os.path.join(resources_dir,'orderedCallLogs.csv')
    load_and_clean_users(users_file,conn)
    load_and_clean_call_logs(call_logs_file,conn)
    write_user_analytics(user_analytics_file,conn)
    write_ordered_calls(ordered_call_logs_file,conn)
    conn.close()




if __name__ == '__main__':
    main()
