#include <iostream>
#include <ctime>

#include "DBAdapter.h"

using namespace std;

int main() {
	//// ******* mysql test *******
	//DBAdapter adapter;
	//string host_name = "localhost";
	//string port_name = "5432";
	//string db_name = "test";
	//string user = "root";
	//string password = "root";
	//string mysql_sql_string = "Select * from circle_test_1";
	//
	//adapter.initialize();
	//adapter.make_connection(
	//	host_name,
	//	port_name,
	//	db_name,
	//	user,
	//	password
	//);
	//adapter.excute_sql(mysql_sql_string);
	//// adapter.show_SQL_result();
	//
	//while (adapter.has_record()) {
	//	vector<string> row;
	//	adapter.fetch_row(row);
	//	for (auto n : row)
	//		cout << n << " ";
	//	cout << endl;
	//}
	//adapter.uninitialize();



	// ******* postgres test *******
	DBAdapter adapter;
	string host_name = "localhost";
	string port_name = "5432";
	string schema_name = "public";
	string db_name = "test_db";
	string user = "postgres";
	string password = "root";
	string postgres_sql_string = "Select * from pg_ado_test_2";

	adapter.initialize();
	adapter.make_connection(
		host_name,
		port_name,
		schema_name,
		db_name,
		user,
		password
	);
	adapter.excute_sql(postgres_sql_string);

	time_t beg_t, end_t;
	time(&beg_t);
	adapter.show_SQL_result();
	time(&end_t);
	cout << "run time: " << end_t - beg_t << " s." << endl;

	adapter.uninitialize();



	return 0;
}
