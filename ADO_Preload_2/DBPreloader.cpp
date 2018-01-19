#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <wchar.h>
#include <io.h>
#include <direct.h>
#include <Windows.h>

#include "cilk/cilk.h"
#include "Db/dbapi.h"
#include "DBPreloader.h"
#include "DBAdapter.h"

/*********************************
***	  some macro definitions   ***
*********************************/
#define MAX_SIZE (MAX_PATH+1)
#define SRC_COLUMN 0
#define DST_COLUMN 1
#define DATE_COLUMN 2
#define MONEY_COLUMN 3
#define RECORD_COLUMN 4
#define DIRECTION_COLUMN 5
#define ROW_SIZE 6
#define parallel_for cilk_for
#define EDGE "Edge"
#define LIGRA "Ligra"
#define ACCOUNT_MAP "Vertexmap"

using std::string;
using std::wstring;
using std::vector;
using std::map;
using std::set;
using std::cout;
using std::endl;


/********************************
***	   non-class functions    ***
********************************/
int API_TimeToStringEX(string &strDateStr, const time_t &timeData) {
	char chTmp[50];
	memset(chTmp, 0, sizeof(chTmp));
	struct tm *p;
	p = localtime(&timeData);
	p->tm_year = p->tm_year + 1900;
	p->tm_mon = p->tm_mon + 1;
	snprintf(chTmp, sizeof(chTmp), "%04d-%02d-%02d %02d:%02d:%02d",
		p->tm_year, p->tm_mon, p->tm_mday, p->tm_hour, p->tm_min, p->tm_sec);
	strDateStr = chTmp;
	return 0;
}

int API_StringToTimeEX(const string &strDateStr, time_t &timeData) {
	char *pBeginPos = (char*)strDateStr.c_str();
	char *pPos = strstr(pBeginPos, "-");
	if (pPos == NULL)
	{
		printf("strDateStr[%s] err \n", strDateStr.c_str());
		return -1;
	}
	int iYear = atoi(pBeginPos);
	int iMonth = atoi(pPos + 1);
	pPos = strstr(pPos + 1, "-");
	if (pPos == NULL)
	{
		printf("strDateStr[%s] err \n", strDateStr.c_str());
		return -1;
	}
	int iDay = atoi(pPos + 1);
	int iHour = 0;
	int iMin = 0;
	int iSec = 0;
	pPos = strstr(pPos + 1, " ");
	if (pPos != NULL)
	{
		iHour = atoi(pPos + 1);
		pPos = strstr(pPos + 1, ":");
		if (pPos != NULL)
		{
			iMin = atoi(pPos + 1);
			pPos = strstr(pPos + 1, ":");
			if (pPos != NULL)
			{
				iSec = atoi(pPos + 1);
			}
		}
	}

	struct tm sourcedate;
	memset((void*)&sourcedate, 0, sizeof(sourcedate));
	sourcedate.tm_sec = iSec;
	sourcedate.tm_min = iMin;
	sourcedate.tm_hour = iHour;
	sourcedate.tm_mday = iDay;
	sourcedate.tm_mon = iMonth - 1;
	sourcedate.tm_year = iYear - 1900;
	timeData = mktime(&sourcedate);
	return 0;
}

// overload for postgreSQL 
std::string get_ligra(
	// connection parameters
	std::string host_name,
	std::string port_name,
	std::string db_name,
	std::string schema_name,
	std::string table_name,
	std::string user,
	std::string password,
	// sql paramsters
	std::string src_account,
	std::string dst_account,
	std::string date,
	std::string money,
	std::string record_id,
	std::string direction,
	std::string cond)
{
	// calculate the file transformation time
	cout << "get ligra-format file..." << endl;
	time_t begin, end;
	begin = time(nullptr);

	DBPreloader loader;
	// create & get dst directory
	string output_dir = "file";
	string dst_directory = loader.create_dst_dir(output_dir);

	// transform original csv
	loader.preload_csv(
		dst_directory,
		// connection params
		host_name,
		port_name,
		db_name,
		schema_name,
		table_name,
		user,
		password,
		// sql params
		src_account,
		dst_account,
		date,
		money,
		record_id,
		direction,
		cond
	);

	// transform ligra
	loader.csv2ligra(dst_directory);

	// rename file
	string ret_time;
	ret_time = loader.rename_file(dst_directory, table_name);

	end = time(nullptr);
	cout << "total running time: " << difftime(end, begin) << " s." << endl;
	cout << "get ligra over...\n" << endl;

	return ret_time;
}

// overload for MySQL 
std::string get_ligra(
	// connection parameters
	std::string host_name,
	std::string port_name,
	std::string db_name,
	std::string table_name,
	std::string user,
	std::string password,
	// sql paramsters
	std::string src_account,
	std::string dst_account,
	std::string date,
	std::string money,
	std::string record_id,
	std::string direction,
	std::string cond)
{
	// calculate the file transformation time
	cout << "get ligra-format file..." << endl;
	time_t begin, end;
	begin = time(nullptr);

	DBPreloader loader;
	// create & get dst directory
	string output_dir = "file";
	string dst_directory = loader.create_dst_dir(output_dir);

	// transform original csv
	loader.preload_csv(
		dst_directory,
		// connection params
		host_name,
		port_name,
		db_name,
		table_name,
		user,
		password,
		// sql params
		src_account,
		dst_account,
		date,
		money,
		record_id,
		direction,
		cond
	);

	// transform ligra
	loader.csv2ligra(dst_directory);

	// rename file
	string ret_time;
	ret_time = loader.rename_file(dst_directory, table_name);

	end = time(nullptr);
	cout << "total running time: " << difftime(end, begin) << " s." << endl;
	cout << "get ligra over...\n" << endl;

	return ret_time;
}


/********************************
***	    utility functions     ***
********************************/
wchar_t* DBPreloader::Utf8_2_Unicode(char* row_i)
{
	int len = MultiByteToWideChar(CP_UTF8, 0, row_i, strlen(row_i), NULL, 0);
	wchar_t *wszStr = new wchar_t[len + 1];
	MultiByteToWideChar(CP_UTF8, 0, row_i, strlen(row_i), wszStr, len);
	wszStr[len] = '\0';
	return wszStr;
}

bool DBPreloader::null_field(const char(&row)[ROW_SIZE][MAX_LENGTH], int row_size) {
	string tmp;
	for (int i = 0; i < row_size; ++i) {
		tmp = format_dquote(row[i]);
		// no empty field
		if (tmp == "") {
			return true;
		}
		// contains no ','
		else if (tmp.find(',') != string::npos)
			return true;
	}
	return false;
}

string DBPreloader::format_dquote(string field) {
	if (field.find("\"") != string::npos)
		return field.substr(1, field.size() - 2);
	else
		return field;
}

time_t DBPreloader::format_date(string date_word) {
	// remove double quate
	date_word = format_dquote(date_word);

	// format by different delim
	// delim always be punctuation: !"#$%&'()*+,-./:;<=>?@[]^_`{|}~
	bool only_date = true;
	for (auto &ch : date_word) {
		if (ch == ':')
			only_date = false;
		else if (ispunct(ch) || ch == '\\')
			ch = '-';
	}

	// transform time string to long(time_t)
	time_t date_number = time(nullptr);
	if (only_date) {
		// trunc
		date_word = date_word.substr(0, 10);
		// add
		date_word += " 00:00:00";
	}
	API_StringToTimeEX(date_word, date_number);

	return date_number;
}

bool Node::cmp(const Node &a, const Node &b) {
	return a.id < b.id;
}



/********************************
***	   interface functions    ***
********************************/
string DBPreloader::create_dst_dir(string dst_directory) {
	cout << "in create_dst_dir..." << endl;
	// get current directory
	char current_absolute_path[MAX_SIZE];
	getcwd(current_absolute_path, MAX_SIZE);

	// check for the dst directory, create if not exists 
	dst_directory = string(current_absolute_path) + "\\" + dst_directory;
	if (access(dst_directory.c_str(), 0) == -1) {
		int not_created = mkdir(dst_directory.c_str());
		if (not_created) {
			cout << "Creating destination directory failed" << endl;
			return "";
		}
	}
	return dst_directory;
}

string DBPreloader::rename_file(string dst_directory, string table_name) {
	cout << "in rename_file..." << endl;

	string old_name, new_name;
	string current_time;
	time_t now = time(nullptr);
	current_time = std::to_string(now);

	// Edge file
	old_name = dst_directory + string("\\") + EDGE;
	new_name = dst_directory + string("\\") + EDGE + string("-")
		+ table_name + "-" + current_time + ".csv";
	if (access(old_name.c_str(), 0) == 0) {
		if (rename(old_name.c_str(), new_name.c_str()) != 0) {
			cout << "rename error!" << endl;
			return "";
		}
	}

	// Map file
	old_name = dst_directory + string("\\") + ACCOUNT_MAP;
	new_name = dst_directory + string("\\") + ACCOUNT_MAP + string("-")
		+ table_name + "-" + current_time + ".csv";
	if (access(old_name.c_str(), 0) == 0) {
		if (rename(old_name.c_str(), new_name.c_str()) != 0) {
			cout << "rename error!" << endl;
			return "";
		}
	}

	// Ligra file
	old_name = dst_directory + string("\\") + LIGRA;
	new_name = dst_directory + string("\\") + LIGRA + string("-")
		+ table_name + "-" + current_time + ".csv";
	if (access(old_name.c_str(), 0) == 0) {
		if (rename(old_name.c_str(), new_name.c_str()) != 0) {
			cout << "rename error!" << endl;
			return "";
		}
	}

	return current_time;
}

/**
* preload_csv
* transform the raw DB data(MySQL) into EDGE file
*
* a few assumptions:
* 1. the format of each line of file is fixed (8 fields)
* 2. each field is embraced by double quate comma
* 3. all transaction years range from "1970-01-01" to "2038-1-19"
* */
// overload for MySQL
void DBPreloader::preload_csv(
	std::string dst_directory,
	// connection parameters
	std::string host_name,
	std::string port_name,
	std::string db_name,
	std::string table_name,
	std::string user,
	std::string password,
	// sql paramsters
	std::string src_account,
	std::string dst_account,
	std::string date,
	std::string money,
	std::string record_id,
	std::string direction,
	std::string cond) 
{
	cout << "in preload_csv..." << endl;

	std::cout << ">>>>> start: ";
	time_t now;
	std::time(&now);
	std::cout << ctime(&now);

	DBAdapter adapter;
	// initialize the environment
	adapter.initialize();	

	// connect MySql database
	adapter.make_connection(
		host_name,
		port_name,
		db_name,
		user,
		password
	);

	// excute sql query
	// pay attention to the space & CANNOT change the sequence of the reading parameters
	string mysql_query = "select " + src_account + "," + dst_account + "," + date + ","
		+ money + "," + record_id + "," + direction + " from " + table_name + " " + cond;
	adapter.excute_sql(mysql_query);

	// fetch query result data AND write into file
	string src_string, dst_string;
	string raw_money, raw_record;
	string raw_direction;
	time_t raw_date = time(nullptr);
	int index = 0, src_index = 0, dst_index = 0;
	int node_number = 0, edge_number = 0;
	map<string, int> account2index;
	string dst_output = dst_directory + "\\" + EDGE;
	std::ofstream ofile(dst_output.c_str());
	ofile << string(40, ' ') << endl;   // for first line

	char row[ROW_SIZE][MAX_LENGTH] = { 0 };
	while (adapter.has_record()) {
		// get the row of the query result
		// since the code of DB is utf-8, here using wstring
		adapter.fetch_row(row);

		// check null filed
		if (null_field(row, ROW_SIZE))
			continue;

		// format date
		raw_date = format_date(row[DATE_COLUMN]);

		// format src account
		src_string = row[SRC_COLUMN];
		src_string = format_dquote(src_string);

		if (account2index.find(src_string) == account2index.end()) {
			src_index = index;
			account2index.insert(map<string, int>::value_type(src_string, index));
			index++;
		}
		else
			src_index = account2index[src_string];

		// format dst account
		dst_string = row[DST_COLUMN];
		dst_string = format_dquote(dst_string);
		if (account2index.find(dst_string) == account2index.end()) {
			dst_index = index;
			account2index.insert(map<string, int>::value_type(dst_string, index));
			index++;
		}
		else
			dst_index = account2index[dst_string];

		// format money
		raw_money = row[MONEY_COLUMN];
		raw_money = format_dquote(raw_money);

		// format record_id
		raw_record = row[RECORD_COLUMN];
		raw_record = format_dquote(raw_record);

		// Shawn's code
		// format forward
		raw_direction = row[DIRECTION_COLUMN];
		raw_direction = format_dquote(raw_direction);

		// write csv file 
		string in = "进";
		string out = "出";
		if (raw_direction == in) {
			ofile << dst_index << ',' << src_index << ','
				<< raw_date << ',' << raw_money << ',' << raw_record << endl;
		}
		else {
			ofile << src_index << ',' << dst_index << ','
				<< raw_date << ',' << raw_money << ',' << raw_record << endl;
		}
		edge_number++;
	}

	// release the ADO resources
	adapter.uninitialize();

	// modify first line
	node_number = account2index.size();
	ofile.seekp(0, std::ios::beg);
	ofile << node_number << "," << edge_number << "," << "FIELD_END";
	ofile.close();

	// write map file
	dst_output = dst_directory + "\\" + ACCOUNT_MAP;
	ofile.open(dst_output);
	for (map<string, int>::iterator iter = account2index.begin(); iter != account2index.end(); iter++)
		ofile << iter->second << "," << "No" << iter->first << endl;
	ofile.close();


	std::cout << ">>>>> end: ";
	std::time(&now);
	std::cout << ctime(&now) << endl;
}

// overload for postgreSQL
void DBPreloader::preload_csv(
	std::string dst_directory,
	// connection parameters
	std::string host_name,
	std::string port_name,
	std::string db_name,
	std::string schema_name,
	std::string table_name,
	std::string user,
	std::string password,
	// sql paramsters
	std::string src_account,
	std::string dst_account,
	std::string date,
	std::string money,
	std::string record_id,
	std::string direction,
	std::string cond)
{
	cout << "in preload_csv..." << endl;

	std::cout << ">>>>> start: ";
	time_t now;
	std::time(&now);
	std::cout << ctime(&now);

	DBAdapter adapter;
	// initialize the environment
	adapter.initialize();

	// connect MySql database
	adapter.make_connection(
		host_name,
		port_name,
		schema_name,
		db_name,
		user,
		password
	);

	// excute sql query
	// pay attention to the space & CANNOT change the sequence of the reading parameters
	string mysql_query = "select " + src_account + "," + dst_account + "," + date + ","
		+ money + "," + record_id + "," + direction + " from " + table_name + " " + cond;
	adapter.excute_sql(mysql_query);

	// fetch query result data AND write into file
	string src_string, dst_string;
	string raw_money, raw_record;
	string raw_direction;
	time_t raw_date = time(nullptr);
	int index = 0, src_index = 0, dst_index = 0;
	int node_number = 0, edge_number = 0;
	map<string, int> account2index;
	string dst_output = dst_directory + "\\" + EDGE;
	std::ofstream ofile(dst_output.c_str());
	ofile << string(40, ' ') << endl;   // for first line

	char row[ROW_SIZE][MAXBYTE] = { 0 };
	while (adapter.has_record()) {
		// get the row of the query result
		// since the code of DB is utf-8, here using wstring
		adapter.fetch_row(row);

		// check null filed
		if (null_field(row, ROW_SIZE))
			continue;

		// format date
		raw_date = format_date(row[DATE_COLUMN]);

		// format src account
		src_string = row[SRC_COLUMN];
		src_string = format_dquote(src_string);

		if (account2index.find(src_string) == account2index.end()) {
			src_index = index;
			account2index.insert(map<string, int>::value_type(src_string, index));
			index++;
		}
		else
			src_index = account2index[src_string];

		// format dst account
		dst_string = row[DST_COLUMN];
		dst_string = format_dquote(dst_string);
		if (account2index.find(dst_string) == account2index.end()) {
			dst_index = index;
			account2index.insert(map<string, int>::value_type(dst_string, index));
			index++;
		}
		else
			dst_index = account2index[dst_string];

		// format money
		raw_money = row[MONEY_COLUMN];
		raw_money = format_dquote(raw_money);

		// format record_id
		raw_record = row[RECORD_COLUMN];
		raw_record = format_dquote(raw_record);

		// Shawn's code
		// format forward
		raw_direction = row[DIRECTION_COLUMN];
		raw_direction = format_dquote(raw_direction);

		// write csv file 
		string in = "进";
		string out = "出";
		if (raw_direction == in) {
			ofile << dst_index << ',' << src_index << ','
				<< raw_date << ',' << raw_money << ',' << raw_record << endl;
		}
		else {
			ofile << src_index << ',' << dst_index << ','
				<< raw_date << ',' << raw_money << ',' << raw_record << endl;
		}
		edge_number++;
	}

	// release the ADO resources
	adapter.uninitialize();

	// modify first line
	node_number = account2index.size();
	ofile.seekp(0, std::ios::beg);
	ofile << node_number << "," << edge_number << "," << "FIELD_END";
	ofile.close();

	// write map file
	dst_output = dst_directory + "\\" + ACCOUNT_MAP;
	ofile.open(dst_output);
	for (map<string, int>::iterator iter = account2index.begin(); iter != account2index.end(); iter++)
		ofile << iter->second << "," << "No" << iter->first << endl;
	ofile.close();

	std::cout << ">>>>> end: ";
	std::time(&now);
	std::cout << ctime(&now) << endl;
}

void DBPreloader::csv2ligra(string dst_directory) {
	cout << "in csv2ligra..." << endl;

	cout << ">>>>> start: ";
	time_t now;
	time(&now);
	cout << ctime(&now);

	void *db, *rec;

	// attach db of shm 1000
	db = wg_attach_existing_database("1001");
	if (NULL == db) {
		db = wg_attach_database("1001", 300000000);
	}

	if (db == NULL) {
		cout << "DBERR: db attach failed" << endl;
		return;
	}

	// check if preload_csv exists 
	string src_csv = dst_directory + "\\" + EDGE;
	if (access(src_csv.c_str(), 0) == -1) {
		cout << "Preloaded csv file does not exist!" << endl;
		return;
	}

	// import preloaded csv file
	char *csv_file = const_cast<char*>(src_csv.c_str());
	if ((rec = wg_get_first_record(db)) == NULL) {
		wg_import_db_csv(db, csv_file);
		cout << "---csv file loaded" << endl;
	}

	// read data from db & transform into ligra graph format
	int nodeNum = 0, edgeNum = 0;
	rec = wg_get_first_record(db);
	nodeNum = wg_decode_int(db, wg_get_field(db, rec, 0));
	edgeNum = wg_decode_int(db, wg_get_field(db, rec, 1));

	// loop , using two arrays to record dst node and differece value of node
	int edgeCount = 0;
	vector<int> diff(nodeNum + 1);
	vector<int> dst(edgeNum);
	vector<Node> ligra_nodes(nodeNum);
	parallel_for(int i = 0; i<nodeNum; i++) {
	// for (int i = 0; i<nodeNum; i++) {
		// construct ligra node
		int dstNode = -1;
		void *rec2 = wg_find_record_int(db, 0, WG_COND_EQUAL, i, NULL);
		while (rec2) {
			dstNode = wg_decode_int(db, wg_get_field(db, rec2, 1));
			// record dst node of src node j
			ligra_nodes[i].dst.insert(dstNode);
			rec2 = wg_find_record_int(db, 0, WG_COND_EQUAL, i, rec2);
		}
		ligra_nodes[i].id = i;
		ligra_nodes[i].offset = ligra_nodes[i].dst.size();
	}

	std::cout << ">>>>> end: ";
	std::time(&now);
	std::cout << ctime(&now) << endl;

	// sort
	std::sort(ligra_nodes.begin(), ligra_nodes.end(), Node::cmp);
	// merge
	diff[0] = 0;
	for (int i = 0; i<(int)ligra_nodes.size(); i++) {
		diff[i + 1] = diff[i] + ligra_nodes[i].offset;
		for (auto iter = ligra_nodes[i].dst.begin(); iter != ligra_nodes[i].dst.end(); ++iter)
			dst[edgeCount++] = *iter;
	}

	string buf = "AdjacencyGraph";
	string dst_ligra = dst_directory + "\\" + LIGRA;
	std::ofstream ofile(dst_ligra.c_str());
	ofile << buf << endl;
	ofile << nodeNum << endl;
	ofile << edgeCount << endl;
	for (int i = 0; i<nodeNum; i++) {
		ofile << diff[i] << endl;
	}

	for (int i = 0; i<edgeCount; i++) {
		ofile << dst[i] << endl;
	}

	ofile.close();
	wg_detach_database(db);
	wg_delete_database("1001");  // need to uncomment after debug
}

