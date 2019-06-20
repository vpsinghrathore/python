# first python program
from flask import Flask
from flask import jsonify
from flask import request
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging.config

logging.basicConfig(filename='birthday_service.log',filemode='a',format='%(asctime)s | %(levelname)s | %(message)s',datefmt='%H:%M:%S',level=logging.DEBUG)
logger = logging.getLogger(__name__)
app = Flask(__name__)

def user_sanity_check(username, dob):
    print("Entered in user_sanity_check()...")
    #Check for digit test in username
    if username.isdigit() == False:
        return 1
    #Check for DOB sanity
    try:
        datetime.datetime.strptime(dob, '%Y-%m-%d')
    except ValueError as error:
        print("Error: Incorrect data format found as it should be [YYYY-MM-DD]!".format(error))
        return 1

    return 0

def get_birthday_days(dob,now):
    print("Entered in get_birthday_days()...")
    print("Received DOB:{0}".format(dob))
    delta1 = datetime(now.year, dob.month, dob.day)
    delta2 = datetime(now.year + 1, dob.month, dob.day)
    days = (max(delta1, delta2) - now).days
    return days

@app.route('/hello/<username>', methods=['PUT'])
def insert_user_birthday(username):
    try:
        print("Parsing received json payload")
        dob = request.json['dateOfBirth']
        print("Received DOB is:{0}".format(dob))

        sanity_status = user_sanity_check(username, dob)
        if sanity_status == 0:
            print("Sanity passed!")
        else:
            print("Sanity Failed!")
            return '', 404
    except Error as error:
        print("Exception while json parsing.Username:{0} and DOB:{1}! Exception: {2}".format(username,dob,error))
        return '', 404

    try:
        connection = mysql.connector.connect(host="100.122.207.148", database="test", user="pagerduty", passwd="paSSw0rd")
        if connection.is_connected():
            db_Info = connection.get_server_info()
            logger.debug("Connected to Shared SRE MariaDB database... MySQL Server version on ".format(db_Info))
            cursor = connection.cursor()

            select_user_details_string = "select count(*) from user_details where username = '" + username + "'"
            insert_user_details_string = "insert into user_details (username, dob) values ('" + username + "','" + dob + "')"
            update_user_details_string = "update user_details set dob = '" + dob + "' where username = '" + username + "'"

            result = cursor.execute(select_user_details_string)
            number_of_rows = result[0]
            #cursor.commit()
            if number_of_rows == 1:
                print("User already exist in DB so updating. Number of count returned {0}".format(number_of_rows))
                cursor.execute(update_user_details_string)
                connection.commit()
                if cursor.rowcount == 1:
                   return '', 204
                else:
                   print("Error while user update.Result:{0}".format(number_of_rows))
                   return '', 404
            else:
                print("User details not found for username: {0} as returned row count is :{1}".format(username,number_of_rows))
                cursor.execute(insert_user_details_string)
                connection.commit()
                if cursor.rowcount == 1:
                    return '', 204
                else:
                    print("Error while user details insertion.Result:{0}".format(number_of_rows))
                    return '', 404
    except Error as error:
        logger.error("Error while connecting to MySQL".format(error))
        return '', 404
    finally:
        # closing database connection.
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            logger.debug("MySQL connection is closed after user DB update!")


@app.route('/hello/<username>', methods=['GET'])
def get_birthday_message(username):
    try:
        connection = mysql.connector.connect(host="100.122.207.148", database="test", user="pagerduty", passwd="paSSw0rd")
        if connection.is_connected():
            db_Info = connection.get_server_info()
            logger.debug("Connected to Shared SRE MariaDB database... MySQL Server version on ".format(db_Info))
            cursor = connection.cursor()
            select_user_details_string = "select dob from user_details where username = '" + username + "'"
            result = cursor.execute(select_user_details_string)
            dob = result[0]
            days = get_birthday_days(dob, datetime.now())
            if days == 0:
                message_value = 'Hello, ' + username + '!Happy Birthday!'
            else:
                message_value = 'Hello, ' + username + '!Your birthday is in ' + days + 'day'

            return jsonify({'message': message_value})
    except Error as error:
        logger.error("Error while connecting to MySQL during get for username:{0}.Exception:".format(username,error))
        return '', 404


############### Main Programm #############
if __name__ == "__main__":
    logger.debug("*************************************************")
    logger.debug("** Starting Birthday API Service built by Shailender **")
    logger.debug("*************************************************")
    app.run(host='127.0.0.1',debug=True, port=5001)
    #app.run(host='0.0.0.0',debug=True, port=5001)
