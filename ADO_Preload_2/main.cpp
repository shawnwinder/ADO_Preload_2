//#include <iostream>
//#include <ctime>
//
//#include "DBAdapter.h"
//
//using namespace std;
//
//int main() {
//	//// ******* mysql test *******
//	//DBAdapter adapter;
//	//string host_name = "localhost";
//	//string port_name = "5432";
//	//string db_name = "test";
//	//string user = "root";
//	//string password = "root";
//	//string mysql_sql_string = "Select * from circle_test_1";
//	//
//	//adapter.initialize();
//	//adapter.make_connection(
//	//	host_name,
//	//	port_name,
//	//	db_name,
//	//	user,
//	//	password
//	//);
//	//adapter.excute_sql(mysql_sql_string);
//	//// adapter.show_SQL_result();
//	//
//	//while (adapter.has_record()) {
//	//	vector<string> row;
//	//	adapter.fetch_row(row);
//	//	for (auto n : row)
//	//		cout << n << " ";
//	//	cout << endl;
//	//}
//	//adapter.uninitialize();
//
//
//
//	// ******* postgres test *******
//	DBAdapter adapter;
//	string host_name = "localhost";
//	string port_name = "5432";
//	string schema_name = "public";
//	string db_name = "test_db";
//	string user = "postgres";
//	string password = "root";
//	string postgres_sql_string = "Select * from pg_ado_test_2";
//
//	adapter.initialize();
//	adapter.make_connection(
//		host_name,
//		port_name,
//		schema_name,
//		db_name,
//		user,
//		password
//	);
//	adapter.excute_sql(postgres_sql_string);
//
//	time_t beg_t, end_t;
//	time(&beg_t);
//	adapter.show_SQL_result();
//	time(&end_t);
//	cout << "run time: " << end_t - beg_t << " s." << endl;
//
//	adapter.uninitialize();
//
//
//
//	return 0;
//}



#include <iostream>
#include <string.h>
#include <stdio.h>
using namespace std;


int *getData()
{
	int nums[10] = { 1,2,3,4,5,6,7,8 };
	return nums;
}

int *getData2()
{
	int aaa[10] = { 8,7,6,5,4,3,2,1 };
	return aaa;
}

int main() {

	int *nums = getData();
	int* nums2 = getData2();
	printf("%d,%d,%d\n", nums[0], nums[1], nums[2]);


	return 0;
}