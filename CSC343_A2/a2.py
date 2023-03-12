"""CSC343 Assignment 2 
 
=== CSC343 Winter 2023 === 
Department of Computer Science, 
University of Toronto 
 
This code is provided solely for the personal and private use of 
students taking the CSC343 course at the University of Toronto. 
Copying for purposes other than this use is expressly prohibited. 
All forms of distribution of this code, whether as given or with 
any changes, are expressly prohibited. 
 
Authors: Danny Heap, Marina Tawfik, and Jacqueline Smith 
 
All of the files in this directory and all subdirectories are: 
Copyright (c) 2023 Danny Heap and Jacqueline Smith 
 
=== Module Description === 
 
This file contains the WasteWrangler class and some simple testing functions. 
"""  
  
import datetime as dt  
import psycopg2 as pg  
import psycopg2.extensions as pg_ext  
import psycopg2.extras as pg_extras  
from typing import Optional, TextIO  
  
  
class WasteWrangler:  
    """A class that can work with data conforming to the schema in 
    waste_wrangler_schema.ddl. 
 
    === Instance Attributes === 
    connection: connection to a PostgreSQL database of a waste management 
    service. 
 
    Representation invariants: 
    - The database to which connection is established conforms to the schema 
      in waste_wrangler_schema.ddl. 
    """  
    connection: Optional[pg_ext.connection]  
  
    def __init__(self) -> None:  
        """Initialize this WasteWrangler instance, with no database connection 
        yet. 
        """  
        self.connection = None  
  
    def connect(self, dbname: str, username: str, password: str) -> bool:  
        """Establish a connection to the database <dbname> using the 
        username <username> and password <password>, and assign it to the 
        instance attribute <connection>. In addition, set the search path 
        to waste_wrangler. 
 
        Return True if the connection was made successfully, False otherwise. 
        I.e., do NOT throw an error if making the connection fails. 
 
        >>> ww = WasteWrangler() 
        >>> ww.connect("csc343h-marinat", "marinat", "") 
        True 
        >>> # In this example, the connection cannot be made. 
        >>> ww.connect("invalid", "nonsense", "incorrect") 
        False 
        """  
        try:  
            self.connection = pg.connect(  
                dbname=dbname, user=username, password=password,  
                options="-c search_path=waste_wrangler"  
            )  
            return True  
        except pg.Error:  
            return False  
  
    def disconnect(self) -> bool:  
        """Close this WasteWrangler's connection to the database. 
 
        Return True if closing the connection was successful, False otherwise. 
        I.e., do NOT throw an error if closing the connection failed. 
 
        >>> ww = WasteWrangler() 
        >>> ww.connect("csc343h-marinat", "marinat", "") 
        True 
        >>> ww.disconnect() 
        True 
        """  
        try:  
            if self.connection and not self.connection.closed:  
                self.connection.close()  
            return True  
        except pg.Error:  
            return False  
  
    def schedule_trip(self, rid: int, time: dt.datetime) -> bool:  
        """Schedule a truck and two employees to the route identified 
        with <rid> at the given time stamp <time> to pick up an 
        unknown volume of waste, and deliver it to the appropriate facility. 
 
        The employees and truck selected for this trip must be available: 
            * They can NOT be scheduled for a different trip from 30 minutes 
              of the expected start until 30 minutes after the end time of this 
              trip. 
            * The truck can NOT be scheduled for maintenance on the same day. 
 
        The end time of a trip can be computed by assuming that all trucks 
        travel at an average of 5 kph. 
 
        From the available trucks, pick a truck that can carry the same 
        waste type as <rid> and give priority based on larger capacity and 
        use the ascending order of ids to break ties. 
 
        From the available employees, give preference based on hireDate 
        (employees who have the most experience get priority), and order by 
        ascending order of ids in case of ties, such that at least one 
        employee can drive the truck type of the selected truck. 
 
        Pick a facility that has the same waste type a <rid> and select the one 
        with the lowest fID. 
 
        Return True iff a trip has been scheduled successfully for the given 
            route. 
        This method should NOT throw an error i.e. if scheduling fails, the 
        method should simply return False. 
 
        No changes should be made to the database if scheduling the trip fails. 
 
        Scheduling fails i.e., the method returns False, if any of the following 
        is true: 
            * If rid is an invalid route ID. 
            * If no appropriate truck, drivers or facility can be found. 
            * If a trip has already been scheduled for <rid> on the same day 
              as <time> (that encompasses the exact same time as <time>). 
            * If the trip can't be scheduled within working hours i.e., between 
              8:00-16:00. 
 
        While a realistic use case will provide a <time> in the near future, our 
        tests could use any valid value for <time>. 
        """  
        try:  
            # TODO: implement this method  
            pass  
        except pg.Error as ex:  
            # You may find it helpful to uncomment this line while debugging,  
            # as it will show you all the details of the error that occurred:  
            # raise ex  
            return False  
  
    def schedule_trips(self, tid: int, date: dt.date) -> int:  
        """Schedule the truck identified with <tid> for trips on <date> using 
        the following approach: 
 
            1. Find routes not already scheduled for <date>, for which <tid> 
               is able to carry the waste type. Schedule these by ascending 
               order of rIDs. 
 
            2. Starting from 8 a.m., find the earliest available pair 
               of drivers who are available all day. Give preference 
               based on hireDate (employees who have the most 
               experience get priority), and break ties by choosing 
               the lower eID, such that at least one employee can 
               drive the truck type of <tid>. 
 
               The facility for the trip is the one with the lowest fID that can 
               handle the waste type of the route. 
 
               The volume for the scheduled trip should be null. 
 
            3. Continue scheduling, making sure to leave 30 minutes between 
               the end of one trip and the start of the next, using the 
               assumption that <tid> will travel an average of 5 kph. 
               Make sure that the last trip will not end after 4 p.m. 
 
        Return the number of trips that were scheduled successfully. 
 
        Your method should NOT raise an error. 
 
        While a realistic use case will provide a <date> in the near future, our 
        tests could use any valid value for <date>. 
        """  
        # TODO: implement this method  
        pass  
  
    def update_technicians(self, qualifications_file: TextIO) -> int:  
        """Given the open file <qualifications_file> that follows the format 
        described on the handout, update the database to reflect that the 
        recorded technicians can now work on the corresponding given truck type. 
 
        For the purposes of this method, you may assume that no two employees 
        in our database have the same name i.e., an employee can be uniquely 
        identified using their name. 
 
        Your method should NOT throw an error. 
        Instead, only correct entries should be reflected in the database. 
        Return the number of successful changes, which is the same as the number 
        of valid entries. 
        Invalid entries include: 
            * Incorrect employee name. 
            * Incorrect truck type. 
            * The technician is already recorded to work on the corresponding 
              truck type. 
            * The employee is a driver. 
 
        Hint: We have provided a helper _read_qualifications_file that you 
            might find helpful for completing this method. 
        """  
        try:  
            # TODO: implement this method  
            cur = self.connection.cursor()  
  
            technician_data = WasteWrangler._read_qualifications_file(qualifications_file)  
              
            num_successful_changes = 0  
            for info in technician_data:  
                print(info)  
                #if(len(info) == 3):  
                firstname =  info[0]  
                surname = info[1]  
                fullname = str(firstname + " " + surname)  
                print(fullname)  
                truck_type = info[2]  
                eid = -1  
                '''if(len(info) == 4): 
                    firstname =  info[1] 
                    surname = info[2] 
                    truck_type = info[3]'''  
  
                # Check if the given employee name is already in the database  
                cur.execute("Select eid from employee where name = '%s';" % (fullname))  
                row = cur.fetchone()  
                if ( row == None):  
                    print("No employee with that name")  
                    continue  
                      
                else:  
                    eid = row  
                    print("There is an employee with that name")  
  
                # Check if the given trucktype is a valid trucktype  
                cur.execute("Select * from trucktype where trucktype = %s;", truck_type)  
                row1 = cur.fetchone()  
                if (row1 == None):  
                    print("truck type does not exist")  
                    continue  
  
                print("trucktype exists")  
  
                # Check if the given employee is a driver  
                cur.execute("Select * from driver where eid = %s;", eid)  
                row2 = cur.fetchone()  
                if(row2 !=None):  
                    print("they are a driver")  
                    continue  
                  
                print("they are not a driver")  
  
                # Check if the current technician is already qualified to do maintenance on that truck type  
                cur.execute("Select * from technician where eid = %s and trucktype = %s;", (eid, truck_type))  
                row3 = cur.fetchone()  
                if (row3 !=None):  
                    print("the technician already does that kind of truck")  
                    continue  
                  
                print("technician does not do that kind of truck")  
  
                # If it passed all above cases then we can insert new entry into technician table and increase count for number of successful changes  
                cur.execute("INSERT INTO technician VALUES (%s,%s);", (eid, truck_type))  
                print ("inserted new technician")  
                num_successful_changes +=1  
                print(num_successful_changes)  
                  
            self.connection.commit()  
            cur.close()  
            return num_successful_changes  
            pass  
        except pg.Error as ex:  
            # You may find it helpful to uncomment this line while debugging,  
            # as it will show you all the details of the error that occurred:  
            # raise ex  
            return 0  
  
    def workmate_sphere(self, eid: int) -> list[int]:  
  
        """Return the workmate sphere of the driver identified by <eid>, as a 
 
        list of eIDs. 
 
  
 
        The workmate sphere of <eid> is: 
 
            * Any employee who has been on a trip with <eid>. 
 
            * Recursively, any employee who has been on a trip with an employee 
 
              in <eid>'s workmate sphere is also in <eid>'s workmate sphere. 
 
  
 
        The returned list should NOT include <eid> and should NOT include 
 
        duplicates. 
 
  
 
        The order of the returned ids does NOT matter. 
 
  
 
        Your method should NOT return an error. If an error occurs, your method 
 
        should simply return an empty list. 
 
        """  
  
        try:  
  
            # Make a cursor and then store the table values of all the employee 1 and 2 IDs into cur1  
  
            cur = self.connection.cursor()  
  
            # Get all the tuples where either the eid1 or eid2 is the eid  
  
            cur.execute("SELECT eID1, eID2 FROM Trip WHERE eID1 = %s OR eID2 = %s;", (eid, eid))              
  
             
  
            # All rows from the select statement are contained in rows  
  
            rows = cur.fetchall()  
  
             
  
            # Initialize the workmate sphere with the employees who have been on a trip with <eid>:  
  
            sphere = set()  
  
            for row in rows:  
  
                if row[1] == eid:  
  
                    sphere.add(row[0])  
  
                elif row[0] == eid:  
  
                    sphere.add(row[1])  
  
   
  
            if len(sphere) == 0:  
  
                return []  
  
   
  
            # Recursively add employees who have been on a trip with an employee in the current workmate sphere:  
  
            # This loop queries the database to find employees who have been on trips with the employees in the current  
  
            # workmate sphere (the employees found in the previous iteration(s) of the loop).  
  
   
  
            while (1):  
  
                # Use a query to find all employees who have been on a trip with an employee in the current workmate sphere,  
  
                # but who have not been on a  
  
                # trip with the current employee (eid).  
  
                lengthSphere = len(sphere)  
  
   
  
                cur.execute("SELECT eID1, eID2 FROM Trip WHERE (eID1 IN %s OR eID2 IN %s);", (tuple(sphere), tuple(sphere)))  
  
                new_workmates = set()  
  
                for row in cur.fetchall():  
  
                    if row[1] in sphere:  
  
                        new_workmates.add(row[0])  
  
                    elif row[0] in sphere:  
  
                        new_workmates.add(row[1])  
  
                sphere = sphere.union(new_workmates)  
  
                newLengthSphere = len(sphere)  
  
   
  
                if newLengthSphere == lengthSphere:  
  
                    break  
  
   
  
            # Remove <eid> and duplicates from the workmate sphere and return the result as a list:  
  
            sphere.discard(eid)  
  
   
  
            # Commit + close  
  
            self.connection.commit()  
  
            cur.close()  
  
   
  
            return list(sphere)  
  
        except pg.Error as ex:  
  
            # You may find it helpful to uncomment this line while debugging,  
  
            # as it will show you all the details of the error that occurred:  
  
            # raise ex  
  
            print(ex)  
  
            return []  
  
    def schedule_maintenance(self, date: dt.date) -> int:  
        """For each truck whose most recent maintenance before <date> happened 
        over 90 days before <date>, and for which there is no scheduled 
        maintenance up to 10 days following date, schedule maintenance with 
        a technician qualified to work on that truck in ascending order of tIDs. 
 
        For example, if <date> is 2023-05-02, then you should consider trucks 
        that had maintenance before 2023-02-01, and for which there is no 
        scheduled maintenance from 2023-05-02 to 2023-05-12 inclusive. 
 
        Choose the first day after <date> when there is a qualified technician 
        available (not scheduled to maintain another truck that day) and the 
        truck is not scheduled for a trip or maintenance on that day. 
 
        If there is more than one technician available on a given day, choose 
        the one with the lowest eID. 
 
        Return the number of trucks that were successfully scheduled for 
        maintenance. 
 
        Your method should NOT throw an error. 
 
        While a realistic use case will provide a <date> in the near future, our 
        tests could use any valid value for <date>. 
        """  
        try:  
            cur = self.connection.cursor()  
            count = 0  
            print(date)  
            list_90days = []  
            list_10days_future = []  
            # All trucks who did not have maintenance in the last 90 days compared to the given date HAVW To FIX DATE THING  
            cur.execute("Select * from maintenance where mdate < date(%s) - interval '90 days';", (date,))  
            for record in cur:  
                if record == 0:  
                    print("no record")  
                list_90days.append(record[0])  
  
            print(list_90days)  
  
            # All trucks who have maintenace in the next 10 days  
            cur.execute("Select * from maintenance where mdate > date(%s) + interval '10 days';", (date,))  
            for record in cur:  
                if record ==0:  
                    print("no record")  
                list_10days_future.append(record[0])  
  
            print(list_10days_future)  
  
            need_maintenance = set(list_90days) - set(list_10days_future)  
            need_maintenance =  sorted(need_maintenance)  
            print("these trucks need maintenanc: ", need_maintenance)  
              
              
              
            for truck in need_maintenance:  
                i = 0   
                isvalid = False  
                while (isvalid == False):  
                    i+=1  
                    next_day = date + dt.timedelta(days=i)  
                    print(next_day)  
                    print(truck)  
                    cur.execute("Select * From trip where tid = %s and date(ttime) = %s;",(truck,next_day))  
                     
                    if(cur.rowcount == 0):  
                        print("there is no trip today)")  
                          
                        cur.execute("Select trucktype from truck where tid = %s;", (truck,))  
                        truck_type = cur.fetchone()  
                        print(truck_type)  
                        print(truck_type[0])  
                          
                        # Find technicians who can work on this truck  
                        technicians = []  
                        cur.execute("Select * from technician where trucktype = %s;", truck_type[0])  
                        technicians = cur.fetchall()  
                        technicians.sort()  
                          
                        print(technicians)  
  
                        for tech in technicians:  
                            print(tech[0])  
                            print(next_day)  
                            cur.execute("Select * from maintenance where eid = %s and mdate = %s;", (tech[0],next_day))  
                            print("i did the query")  
                            if(cur.rowcount == 0):  
                                print("I found a day with an available technician and the tech is:",tech[0])  
                                cur.execute("Insert into maintenance values (%s,%s,%s);", (truck,tech[0],next_day))  
                                count +=1  
                                isvalid = True  
                                break  
                      
              
            self.connection.commit()  
            cur.close()  
            return count  
            pass  
        except pg.Error as ex:  
            # You may find it helpful to uncomment this line while debugging,  
            # as it will show you all the details of the error that occurred:  
            # raise ex  
            return 0  
  
    def reroute_waste(self, fid: int, date: dt.date) -> int:  
  
        """Reroute the trips to <fid> on day <date> to another facility that 
 
        takes the same type of waste. If there are many such facilities, pick 
 
        the one with the smallest fID (that is not <fid>). 
 
  
 
        Return the number of re-routed trips. 
 
  
 
        Don't worry about too many trips arriving at the same time to the same 
 
        facility. Each facility has ample receiving facility. 
 
  
 
        Your method should NOT return an error. If an error occurs, your method 
 
        should simply return 0 i.e., no trips have been re-routed. 
 
  
 
        While a realistic use case will provide a <date> in the near future, our 
 
        tests could use any valid value for <date>. 
 
  
 
        Assume this happens before any of the trips have reached <fid>. 
 
        """  
  
        try:  
  
            # TODO: implement this method  
  
            # Get all facilities and their wasteTypes:  
  
            cur = self.connection.cursor()  
  
            cur.execute("SELECT fid, wastetype FROM Facility;")  
  
            rows = cur.fetchall()  
  
   
  
            # Get the one particular valid waste (same as the waste type from fid)  
  
            cur.execute("SELECT wastetype FROM Facility WHERE fid = %s;", (fid, ))  
  
            # Fetch one since you will only get one wasteType  
  
            validWasteType = cur.fetchone()            
  
   
  
            cur.execute("SELECT fid FROM Facility WHERE wastetype = %s AND fid <> %s ORDER BY fid;", (validWasteType, fid))  
  
            valid_facilities = [row[0] for row in cur.fetchall()]  
  
   
  
            # Get all trips that are scheduled to arrive at the given facility on the given date:  
  
            cur.execute("SELECT * FROM Trip WHERE fID = %s AND date(tTIME) = %s;", (fid, date))  
  
            trips_to_reroute = cur.fetchall()  
  
   
  
            # Check if there are trips to reroute:  
  
            if len(trips_to_reroute) == 0:  
  
                return 0  
  
   
  
            # Want the first fID (lowest)  
  
            nice_facility = valid_facilities[0]  
  
   
  
            # Reroute trips to nice_facility  
  
            count = 0  
  
             
  
            for trip in trips_to_reroute:  
  
                 rID, tID, tTIME, volume, eID1, eID2, fID = trip  
  
                 cur.execute("UPDATE TRIP SET fID = %s WHERE rid = %s AND ttime = %s;", (nice_facility, rID, tTIME))  
  
                 count = count + 1  
  
             
  
            self.connection.commit()  
  
            cur.close()  
  
            return count  
  
   
  
            pass  
  
        except pg.Error as ex:  
  
            # You may find it helpful to uncomment this line while debugging,  
  
            # as it will show you all the details of the error that occurred:  
  
            # raise ex  
  
            return 0  
  
    # =========================== Helper methods ============================= #  
 
    @staticmethod  
    def _read_qualifications_file(file: TextIO) -> list[list[str, str, str]]:  
        """Helper for update_technicians. Accept an open file <file> that 
        follows the format described on the A2 handout and return a list 
        representing the information in the file, where each item in the list 
        includes the following 3 elements in this order: 
            * The first name of the technician. 
            * The last name of the technician. 
            * The truck type that the technician is currently qualified to work 
              on. 
 
        Pre-condition: 
            <file> follows the format given on the A2 handout. 
        """  
        result = []  
        employee_info = []  
        for idx, line in enumerate(file):  
            if idx % 2 == 0:  
                info = line.strip().split(' ')[-2:]  
                fname, lname = info  
                employee_info.extend([fname, lname])  
            else:  
                employee_info.append(line.strip())  
                result.append(employee_info)  
                employee_info = []  
  
        return result  
  
  
def setup(dbname: str, username: str, password: str, file_path: str) -> None:  
    """Set up the testing environment for the database <dbname> using the 
    username <username> and password <password> by importing the schema file 
    and the file containing the data at <file_path>. 
    """  
    connection, cursor, schema_file, data_file = None, None, None, None  
    try:  
        # Change this to connect to your own database  
        connection = pg.connect(  
            dbname=dbname, user=username, password=password,  
            options="-c search_path=waste_wrangler"  
        )  
        cursor = connection.cursor()  
  
        schema_file = open("./waste_wrangler_schema.sql", "r")  
        cursor.execute(schema_file.read())  
  
        data_file = open(file_path, "r")  
        cursor.execute(data_file.read())  
  
        connection.commit()  
    except Exception as ex:  
        connection.rollback()  
        raise Exception(f"Couldn't set up environment for tests: \n{ex}")  
    finally:  
        if cursor and not cursor.closed:  
            cursor.close()  
        if connection and not connection.closed:  
            connection.close()  
        if schema_file:  
            schema_file.close()  
        if data_file:  
            data_file.close()  
  
  
def test_preliminary() -> None:  
    """Test preliminary aspects of the A2 methods."""  
    ww = WasteWrangler()  
    qf = None  
    try:  
        # TODO: Change the values of the following variables to connect to your  
        #  own database:  
        dbname = 'csc343h-sivayo30'  
        user = 'sivayo30'  
        password = ''  
  
        connected = ww.connect(dbname, user, password)  
  
        # The following is an assert statement. It checks that the value for  
        # connected is True. The message after the comma will be printed if  
        # that is not the case (connected is False).  
        # Use the same notation to thoroughly test the methods we have provided  
        assert connected, f"[Connected] Expected True | Got {connected}."  
  
        # TODO: Test one or more methods here, or better yet, make more testing  
        #   functions, with each testing a different aspect of the code.  
  
        # The following function will set up the testing environment by loading  
        # the sample data we have provided into your database. You can create  
        # more sample data files and use the same function to load them into  
        # your database.  
        # Note: make sure that the schema and data files are in the same  
        # directory (folder) as your a2.py file.  
        setup(dbname, user, password, './waste_wrangler_data.sql')  
          
  
       # --------------------- Testing schedule_trip  ------------------------#  
  
        # You will need to check that data in the Trip relation has been  
        # changed accordingly. The following row would now be added:  
        # (1, 1, '2023-05-04 08:00', null, 2, 1, 1)  
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 8, 0))  
        assert scheduled_trip, \  
            f"[Schedule Trip] Expected True, Got {scheduled_trip}"  
  
        # Can't schedule the same route of the same day.  
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 13, 0))  
        assert not scheduled_trip, \  
            f"[Schedule Trip] Expected False, Got {scheduled_trip}"  
  
        # -------------------- Testing schedule_trips  ------------------------#  
  
        # All routes for truck tid are scheduled on that day  
        scheduled_trips = ww.schedule_trips(1, dt.datetime(2023, 5, 3))  
        assert scheduled_trips == 0, \  
            f"[Schedule Trips] Expected 0, Got {scheduled_trips}"   
              
          
  
        # ----------------- Testing update_technicians  -----------------------#  
  
        # This uses the provided file. We recommend you make up your custom  
        # file to thoroughly test your implementation.  
        # You will need to check that data in the Technician relation has been  
        # changed accordingly  
        qf = open('qualifications.txt', 'r')  
        updated_technicians = ww.update_technicians(qf)  
        assert updated_technicians == 2, \  
            f"[Update Technicians] Expected 2, Got {updated_technicians}"  
      
          
  
        # ----------------- Testing workmate_sphere ---------------------------#  
  
        # This employee doesn't exist in our instance  
        workmate_sphere = ww.workmate_sphere(2023)  
        assert len(workmate_sphere) == 0, \  
            f"[Workmate Sphere] Expected [], Got {workmate_sphere}"  
  
        workmate_sphere = ww.workmate_sphere(3)  
        # Use set for comparing the results of workmate_sphere since  
        # order doesn't matter.  
        # Notice that 2 is added to 1's work sphere because of the trip we  
        # added earlier.  
        assert set(workmate_sphere) == {1, 2}, \  
            f"[Workmate Sphere] Expected {{1, 2}}, Got {workmate_sphere}"  
  
        # ----------------- Testing schedule_maintenance ----------------------#  
  
        # You will need to check the data in the Maintenance relation  
        scheduled_maintenance = ww.schedule_maintenance(dt.date(2023, 5, 5))  
        assert scheduled_maintenance == 7, \  
            f"[Schedule Maintenance] Expected 7, Got {scheduled_maintenance}"  
  
        # ------------------ Testing reroute_waste  ---------------------------#  
  
        # There is no trips to facility 1 on that day  
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 10))  
        assert reroute_waste == 0, \  
            f"[Reroute Waste] Expected 0. Got {reroute_waste}"  
  
        # You will need to check that data in the Trip relation has been  
        # changed accordingly  
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 3))  
        assert reroute_waste == 1, \  
            f"[Reroute Waste] Expected 1. Got {reroute_waste}"  
          
    finally:  
        if qf and not qf.closed:  
            qf.close()  
        ww.disconnect()  
  
  
if __name__ == '__main__':  
    # Un comment-out the next two lines if you would like to run the doctest  
    # examples (see ">>>" in the methods connect and disconnect)  
    # import doctest  
    # doctest.testmod()  
  
    # TODO: Put your testing code here, or call testing functions such as  
    #   this one:  
    test_preliminary()  
  
  
      
