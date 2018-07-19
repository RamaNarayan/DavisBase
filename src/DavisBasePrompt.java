import static java.lang.System.out;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import constants.BtreeConstants;
import constants.ColumnsTableConstants;
import constants.DataTypes;
import utils.BufferManagement;
import utils.FileUtils;

/**
 * @author RAMA NARAYAN LAKSHMANAN
 *
 */
public class DavisBasePrompt {

	static String prompt = "davisql> ";
	static String version = "v1.0";
	static String copyright = "©2018 Rama Narayan Lakshmanan";
	static final String tablesTableFile = "davisbase_tables.tbl";
	static final String columnsTableFile = "davisbase_columns.tbl";
	static final String metadataFolderPath = "data/catalog/";
	static final String userDataFolderPath = "data/user_data/";
	static boolean isExit = false;

	static long pageSize = 512;

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {
		
		File f = new File("data");		
		if(!f.exists()) {
			initializeDataStore();
		}
		else {
			File f1 = new File("data/catalog");
			File f2 = new File("data/user_data");
			if(!f1.exists()||!f2.exists()) {
				initializeDataStore();
			}
			else {
				f1 = new File(metadataFolderPath+tablesTableFile);
				f2 = new File(metadataFolderPath+columnsTableFile);
				if(!f1.exists()||!f2.exists()) {
					initializeDataStore();
				}
			}
		}

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		while (!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}
	
	static void initializeDataStore() {
		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
			File catalogDir = new File("data/catalog");
			catalogDir.mkdir();
			File userDataDir = new File("data/user_data");
			userDataDir.mkdir();
		}
		catch (SecurityException se) {
			out.println("Unable to create data container directory");
			out.println(se);
		}

		/** Create davisbase_tables system catalog */
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

			davisbaseTablesCatalog.setLength(pageSize*2);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.writeInt((int)pageSize);
			davisbaseTablesCatalog.seek(pageSize*1);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x00);
			//no right sibling
			davisbaseTablesCatalog.seek(pageSize+BtreeConstants.rightOffset);
			davisbaseTablesCatalog.write(0xFF);
			davisbaseTablesCatalog.write(0xFF);
			davisbaseTablesCatalog.write(0xFF);
			davisbaseTablesCatalog.write(0xFF);
			davisbaseTablesCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_tables file");
			out.println(e);
		}

		/** Create davisbase_columns systems catalog */
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize*2);
			davisbaseColumnsCatalog.seek(0);
			davisbaseColumnsCatalog.writeInt((int)pageSize);
			davisbaseColumnsCatalog.seek(pageSize*1);
			davisbaseColumnsCatalog.write(0x0D);
			davisbaseColumnsCatalog.write(0x00);
			//no right sibling
			davisbaseColumnsCatalog.seek(pageSize+BtreeConstants.rightOffset);
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_columns file");
			out.println(e);
		}
	}

	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}

	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}

	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("CREATE TABLE <table_name> (<value_list>)");
		out.println("\t creates a new table. DO NOT INCLUDE rowid in the attribute list.\n");
		out.println("INSERT INTO <table_name> [(<column_list>)] VALUES (<value_list>)");
		out.println(
				"\t Inserts values into a new table mapping optional <column_list>. DO NOT INCLUDE rowid in the attribute list.\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records satisfying optional <condition>\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data satisfying optional <condition>\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
		case "select":
			parseQuery(userCommand);
			break;
		case "drop":
			dropTable(userCommand);
			break;
		case "create":
			parseCreateTable(userCommand);
			break;
		case "update":
			parseUpdate(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
			break;
		case "show":
			showTables(userCommand);
			break;
		case "insert":
			parseInsert(userCommand);
			break;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	private static void parseInsert(String userCommand) {
		ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" values ")));
		ArrayList<String> metaTokens = new ArrayList<String>(Arrays.asList(insertTokens.get(0).split(" ")));

		ArrayList<String> columnTokens = new ArrayList<String>();
		ArrayList<String> dataArrayList = new ArrayList<String>();
		ArrayList<String> dataTypeList = new ArrayList<String>();
		ArrayList<String> nullConstraintList = new ArrayList<String>();
		ArrayList<String> columnNames = new ArrayList<String>();

		ArrayList<String> dataTokens = new ArrayList<String>();

		Matcher insertMatcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(insertTokens.get(1));
		while (insertMatcher.find()) {
			dataTokens.add(insertMatcher.group(1).replace("\"", ""));
		}
		String tableName = metaTokens.get(2);
		boolean isTablePresent = FileUtils.isTablePresent(metadataFolderPath + tablesTableFile, tableName, pageSize);
		if (isTablePresent) {
			String tableFileName = tableName + ".tbl";
			try {
				RandomAccessFile tableFile = new RandomAccessFile(userDataFolderPath + tableFileName, "rw");
				tableFile.seek(BtreeConstants.NoOfColumnsOffsetInDummyPage);
				int noOfTableColumns = tableFile.readInt();
				String columnsFile = metadataFolderPath + columnsTableFile;
				ArrayList<ArrayList<String>> dataTypes = FileUtils.getRecordsFromColumnsTable(columnsFile, tableName,
						noOfTableColumns, pageSize);
				boolean isLegalInsert = true;
				for (int i = 0; i < dataTypes.size(); i++) {

					String columnName = dataTypes.get(i).get(ColumnsTableConstants.column_name);
					columnNames.add(columnName);

					String type = dataTypes.get(i).get(ColumnsTableConstants.data_type);
					dataTypeList.add(type);

					String isNull = dataTypes.get(i).get(ColumnsTableConstants.isNullable);
					nullConstraintList.add(isNull);

				}
				if (metaTokens.size() < 4) {
					for (int i = 1; i < dataTokens.size() - 1; i = i + 2) {
						dataArrayList.add(dataTokens.get(i));
					}
					if (dataArrayList.size() < columnNames.size()) {
						int diff = columnNames.size() - dataArrayList.size();
						for (int i = 0; i < diff; i++) {
							dataArrayList.add("null");
						}
					}

				} else {
					for (int i = 3; i < metaTokens.size(); i++) {
						columnTokens.add(metaTokens.get(i));
					}
					Map<String, String> dataMap = new HashMap<String, String>();
					if (columnTokens.size() == dataTokens.size()) {
						for (int i = 1; i < columnTokens.size() - 1; i = i + 2) {
							dataMap.put(columnTokens.get(i), dataTokens.get(i));
						}
					}
					for (int i = 0; i < columnNames.size(); i++) {
						if (dataMap.containsKey(columnNames.get(i)))
							dataArrayList.add(dataMap.get(columnNames.get(i)));
						else
							dataArrayList.add("null");
					}
				}

				for (int i = 0; i < dataArrayList.size(); i++) {
					if (dataArrayList.get(i).isEmpty() || dataArrayList.get(i).equalsIgnoreCase("null")) {
						if (nullConstraintList.get(i).equalsIgnoreCase("No")) {
							isLegalInsert = false;
							System.out.println("NULL Constraint violation. " + columnNames.get(i) + " Cannot be null");
							break;
						}
					}
				}

				if (isLegalInsert) {
					ArrayList<ArrayList<String>> modifiedList = handleNulls(dataArrayList, dataTypeList);
					dataTypeList = modifiedList.get(1);
					dataArrayList = handleDateTime(modifiedList.get(0), dataTypeList);
					insertRecord(userDataFolderPath + tableFileName, dataArrayList, dataTypeList);
					System.out.println("1 record inserted");
				}
				tableFile.close();
			} catch (Exception e) {
				System.out.println("No such table");
			}
		} else {
			System.out.println("No such table " + tableName);
		}

	}

	public static ArrayList<String> handleDateTime(ArrayList<String> dataArrayList, ArrayList<String> dataTypeList) {
		ArrayList<String> newDataList = new ArrayList<String>();
		for (int i = 0; i < dataArrayList.size(); i++) {
			if (dataTypeList.get(i).equalsIgnoreCase("date") || dataTypeList.get(i).equalsIgnoreCase("datetime")) {
				String datetime;
				if (dataTypeList.get(i).equalsIgnoreCase("date")) {
					datetime = dataArrayList.get(i) + "_00:00:00";
				} else {
					datetime = dataArrayList.get(i);
				}
				Long epoch = FileUtils.getDateTimeInEpoch(datetime);
				newDataList.add(epoch.toString());

			} else {
				newDataList.add(dataArrayList.get(i));
			}
		}
		return newDataList;
	}

	private static ArrayList<ArrayList<String>> handleNulls(ArrayList<String> dataArrayList,
			ArrayList<String> dataTypeList) {
		ArrayList<ArrayList<String>> modifiedList = new ArrayList<ArrayList<String>>();
		ArrayList<String> newDataList = new ArrayList<String>();
		ArrayList<String> newTypeList = new ArrayList<String>();
		for (int i = 0; i < dataTypeList.size(); i++) {
			if (dataArrayList.get(i).isEmpty() || dataArrayList.get(i).equalsIgnoreCase("null")) {
				newDataList.add(dataArrayList.get(i));
				newTypeList.add(DataTypes.getNullTypeForDataType(dataTypeList.get(i)));
			} else {
				newDataList.add(dataArrayList.get(i));
				newTypeList.add(dataTypeList.get(i));
			}
		}
		modifiedList.add(newDataList);
		modifiedList.add(newTypeList);
		return modifiedList;
	}

	private static void showTables(String userCommand) {
		try {
			String tableFileName = metadataFolderPath + tablesTableFile;
			String isActive = "1";
			ArrayList<String> header = new ArrayList<String>();
			header.add("rowid");
			header.add("table_name");
			header.add("is_active");
			ArrayList<ArrayList<String>> tables = FileUtils.getRecordsFromUserTable(tableFileName, isActive, "tinyint",
					2, "=", pageSize);
			ArrayList<Integer> selectColumns = new ArrayList<Integer>();
			selectColumns.add(0);
			selectColumns.add(1);
			printRecordsInConsole(header, tables, selectColumns);

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void printRecordsInConsole(ArrayList<String> header, ArrayList<ArrayList<String>> data,
			ArrayList<Integer> selectColumns) {
		System.out.println(line("-", 80));
		if (selectColumns != null) {
			for (int i = 0; i < selectColumns.size(); i++) {
				System.out.print(header.get(selectColumns.get(i)) + '|' + '\t');
			}
			System.out.println();
			System.out.println(line("-", 80));
			for (int i = 0; i < data.size(); i++) {
				for (int j = 0; j < selectColumns.size(); j++)
					System.out.print(data.get(i).get(selectColumns.get(j)) + '|' + '\t');
				System.out.println();
			}
		} else {
			for (int i = 0; i < header.size(); i++) {
				System.out.print(header.get(i) + '|' + '\t');
			}
			System.out.println();
			System.out.println(line("-", 80));
			for (int i = 0; i < data.size(); i++) {
				for (int j = 0; j < data.get(i).size(); j++) {
					System.out.print(data.get(i).get(j) + '|' + '\t');
				}
				System.out.println();
			}
		}
		System.out.println(line("-", 80));

	}

	public static void dropTable(String dropTableString) {
		ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
		String searchValue = dropTableTokens.get(2);
		if (FileUtils.isTablePresent(metadataFolderPath + tablesTableFile, searchValue, pageSize)) {
			String searchDataType = "text";
			int searchOrdinalPosition = 1;
			String updateValue = "0";
			String updateDataType = "tinyint";
			int updateOrdinalPosition = 2;
			FileUtils.updateRecordsInUserTable(metadataFolderPath + tablesTableFile, searchValue, searchDataType,
					searchOrdinalPosition, updateValue, updateDataType, updateOrdinalPosition, "=", pageSize);
			// update columns table
			updateValue = "0";
			updateDataType = "tinyint";
			updateOrdinalPosition = 6;
			FileUtils.updateRecordsInUserTable(metadataFolderPath + columnsTableFile, searchValue, searchDataType,
					searchOrdinalPosition, updateValue, updateDataType, updateOrdinalPosition, "=", pageSize);
			try {
				RandomAccessFile tableFile = new RandomAccessFile(userDataFolderPath + searchValue + ".tbl", "rw");
				tableFile.setLength(pageSize * 1);
				tableFile.seek(0);
				byte[] b = new byte[(int) pageSize];
				tableFile.write(b, 0, (int) pageSize);
				System.out.println("table dropped");
				tableFile.close();
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println("no such table " + searchValue);
		}
	}

	public static void parseQuery(String queryString) {
		try {

			ArrayList<String> queryTableTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
			ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(queryString.split("from ")));
			ArrayList<String> colTokens = new ArrayList<String>(Arrays.asList(tokens.get(0).split(" ")));
			ArrayList<String> tableTokens;
			ArrayList<String> secondTokens;
			ArrayList<String> conditionTokens = new ArrayList<String>();
			boolean isConditional = false;
			boolean isLegalSelect = true;
			String errorMessage = "";
			if (queryTableTokens.contains("where")) {
				secondTokens = new ArrayList<String>(Arrays.asList(tokens.get(1).split(" where ")));
				tableTokens = new ArrayList<String>(Arrays.asList(secondTokens.get(0).split(" ")));
				conditionTokens = new ArrayList<String>();

				Matcher conditionMatcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(secondTokens.get(1));
				while (conditionMatcher.find()) {
					conditionTokens.add(conditionMatcher.group(1).replace("\"", ""));
				}
				isConditional = true;
			} else {
				tableTokens = new ArrayList<String>(Arrays.asList(tokens.get(1).split(" ")));
			}

			ArrayList<String> requestedColumns = new ArrayList<String>();
			ArrayList<String> header = new ArrayList<String>();
			boolean isSelectAll = colTokens.get(1).equals("*") ? true : false;
			ArrayList<ArrayList<String>> records = new ArrayList<ArrayList<String>>();

			ArrayList<Integer> selectColumns = new ArrayList<Integer>();
			String tableName = tableTokens.get(0);
			boolean isTablePresent = FileUtils.isTablePresent(metadataFolderPath + tablesTableFile, tableName,
					pageSize);
			if (isTablePresent) {
				String tableFileName = tableName + ".tbl";
				RandomAccessFile tableFile = new RandomAccessFile(userDataFolderPath + tableFileName, "rw");
				tableFile.seek(BtreeConstants.NoOfColumnsOffsetInDummyPage);
				int noOfTableColumns = tableFile.readInt();
				String columnsFile = metadataFolderPath + columnsTableFile;
				ArrayList<ArrayList<String>> columnsMeta = FileUtils.getRecordsFromColumnsTable(columnsFile, tableName,
						noOfTableColumns, pageSize);
				header.add("rowid");
				for (int i = 0; i < columnsMeta.size(); i++) {
					header.add(columnsMeta.get(i).get(ColumnsTableConstants.column_name));
				}

				if (!isSelectAll) {
					for (int i = 1; i < colTokens.size(); i += 2) {
						requestedColumns.add(colTokens.get(i));
					}
					for (int i = 0; i < requestedColumns.size(); i++) {
						if (header.contains(requestedColumns.get(i))) {
							selectColumns.add(header.indexOf(requestedColumns.get(i)));
						} else {
							isLegalSelect = false;
							errorMessage = "Column not found: " + requestedColumns.get(i);
						}
					}
				}

				if (isLegalSelect) {
					if (isConditional) {
						String searchColumn = conditionTokens.get(0);
						String operator = conditionTokens.get(1);
						String searchValue = conditionTokens.get(2);
						if (searchColumn.equalsIgnoreCase("rowid")) {
							records = FileUtils.getRecordFromUserTableWithRowid(userDataFolderPath + tableFileName,
									Integer.parseInt(searchValue), operator, pageSize);
							if (isSelectAll)
								printRecordsInConsole(header, records, null);
							else
								printRecordsInConsole(header, records, selectColumns);
						} else {
							String searchDataType = "";
							boolean isFound = false;
							int ordinalPosition = 0;
							for (int i = 0; i < columnsMeta.size(); i++) {
								if (columnsMeta.get(i).get(ColumnsTableConstants.column_name)
										.equalsIgnoreCase(searchColumn)) {
									isFound = true;
									searchDataType = columnsMeta.get(i).get(ColumnsTableConstants.data_type);
									ordinalPosition = Integer
											.parseInt(columnsMeta.get(i).get(ColumnsTableConstants.ordinal_position));
									break;
								}
							}
							if (isFound) {
								if(operator.equalsIgnoreCase("is")) {									
									searchDataType = DataTypes.getNullTypeForDataType(searchDataType);
								}
								if (searchDataType.equalsIgnoreCase("date")
										|| searchDataType.equalsIgnoreCase("datetime")) {
									String datetime;
									if (searchDataType.equalsIgnoreCase("date")) {
										datetime = searchValue + "_00:00:00";
									} else {
										datetime = searchValue;
									}
									Long epoch = FileUtils.getDateTimeInEpoch(datetime);
									searchValue = epoch.toString();
								}
								records = FileUtils.getRecordsFromUserTable(userDataFolderPath + tableFileName,
										searchValue, searchDataType, ordinalPosition, operator, pageSize);
								if (isSelectAll)
									printRecordsInConsole(header, records, null);
								else
									printRecordsInConsole(header, records, selectColumns);
							} else {
								System.out.println("Where condition has a column which is not present");
							}
						}

					}

					else {
						records = FileUtils.getAllRecordsFromUserTable(userDataFolderPath + tableFileName, pageSize);
						if (isSelectAll)
							printRecordsInConsole(header, records, null);
						else
							printRecordsInConsole(header, records, selectColumns);
					}					
				} else {
					System.out.println(errorMessage);
				}
				tableFile.close();
			} else {
				System.out.println("no such table " + tableName);
			}

		} catch (Exception e) {

		}

	}

	public static void parseUpdate(String updateString) {
		try {

			ArrayList<String> queryTableTokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));
			ArrayList<String> tokens = new ArrayList<String>(Arrays.asList(updateString.split("set ")));
			ArrayList<String> tableTokens = new ArrayList<String>(Arrays.asList(tokens.get(0).split(" ")));
			ArrayList<String> setValueTokens;
			ArrayList<String> secondTokens;
			ArrayList<String> conditionTokens = new ArrayList<String>();
			boolean isConditional = false;
			boolean isLegalUpdate = true;
			String errorMessage = "";

			if (queryTableTokens.contains("where")) {
				secondTokens = new ArrayList<String>(Arrays.asList(tokens.get(1).split(" where ")));
				setValueTokens = new ArrayList<String>();
				Matcher setValueMatcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(secondTokens.get(0));
				while (setValueMatcher.find()) {
					setValueTokens.add(setValueMatcher.group(1).replace("\"", ""));
				}
				conditionTokens = new ArrayList<String>();
				Matcher conditionalMatcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(secondTokens.get(1));
				while (conditionalMatcher.find()) {
					conditionTokens.add(conditionalMatcher.group(1).replace("\"", ""));
				}
				isConditional = true;
			} else {
				setValueTokens = new ArrayList<String>();
				Matcher setValueMatcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(tokens.get(1));
				while (setValueMatcher.find()) {
					setValueTokens.add(setValueMatcher.group(1).replace("\"", ""));
				}
			}

			ArrayList<String> header = new ArrayList<String>();

			String tableName = tableTokens.get(1);
			boolean isTablePresent = FileUtils.isTablePresent(metadataFolderPath + tablesTableFile, tableName,
					pageSize);
			if (isTablePresent) {
				String tableFileName = tableName + ".tbl";
				RandomAccessFile tableFile = new RandomAccessFile(userDataFolderPath + tableFileName, "rw");
				tableFile.seek(BtreeConstants.NoOfColumnsOffsetInDummyPage);
				int noOfTableColumns = tableFile.readInt();
				String columnsFile = metadataFolderPath + columnsTableFile;
				ArrayList<ArrayList<String>> columnsMeta = FileUtils.getRecordsFromColumnsTable(columnsFile, tableName,
						noOfTableColumns, pageSize);
				header.add("rowid");
				for (int i = 0; i < columnsMeta.size(); i++) {
					header.add(columnsMeta.get(i).get(ColumnsTableConstants.column_name));
				}
				String updateColumn = setValueTokens.get(0);
				String updateValue = setValueTokens.get(2);
				String updateDataType = "";
				String updateNullConstraint = "";
				int updateOrdinalPosition = 0;
				boolean isFound = false;
				for (int i = 0; i < columnsMeta.size(); i++) {
					if (columnsMeta.get(i).get(ColumnsTableConstants.column_name).equalsIgnoreCase(updateColumn)) {
						isFound = true;
						updateDataType = columnsMeta.get(i).get(ColumnsTableConstants.data_type);
						updateOrdinalPosition = Integer
								.parseInt(columnsMeta.get(i).get(ColumnsTableConstants.ordinal_position));
						updateNullConstraint = columnsMeta.get(i).get(ColumnsTableConstants.isNullable);
						break;
					}
				}

				if (updateValue.equalsIgnoreCase("null") && updateNullConstraint.equalsIgnoreCase("no")) {
					isLegalUpdate = false;
					errorMessage = "Null Constraint violation " + updateColumn + " cannot be null";
				}

				if (updateValue.equalsIgnoreCase("null") && updateNullConstraint.equalsIgnoreCase("yes")) {
					updateDataType = DataTypes.getNullTypeForDataType(updateDataType);
				}

				if (updateDataType.equalsIgnoreCase("date") || updateDataType.equalsIgnoreCase("datetime")) {
					String datetime;
					if (updateDataType.equalsIgnoreCase("date")) {
						datetime = updateValue + "_00:00:00";
					} else {
						datetime = updateValue;
					}
					Long epoch = FileUtils.getDateTimeInEpoch(datetime);
					updateValue = epoch.toString();
				}

				if (isLegalUpdate) {
					if (isConditional) {
						String searchColumn = conditionTokens.get(0);
						String operator = conditionTokens.get(1);
						String searchValue = conditionTokens.get(2);

						if (searchColumn.equalsIgnoreCase("rowid")) {
							int rowid = Integer.parseInt(searchValue);
							int count = FileUtils.updateRecordsInUserTableWithRowId(userDataFolderPath + tableFileName,
									rowid, updateValue, updateDataType, updateOrdinalPosition, operator, pageSize);
							if (count > 0) {
								System.out.println(count + " row(s) updated");
							} else {
								System.out.println("no row(s) updated as no such rowid " + rowid);
							}
						} else {
							String searchDataType = "";
							int searchOrdinalPosition = 0;
							if (isFound) {
								for (int i = 0; i < columnsMeta.size(); i++) {
									if (columnsMeta.get(i).get(ColumnsTableConstants.column_name)
											.equalsIgnoreCase(searchColumn)) {
										isFound = true;
										searchDataType = columnsMeta.get(i).get(ColumnsTableConstants.data_type);
										searchOrdinalPosition = Integer.parseInt(
												columnsMeta.get(i).get(ColumnsTableConstants.ordinal_position));
										break;
									}
								}
								if (isFound) {
									if(operator.equalsIgnoreCase("is")) {
										searchDataType = DataTypes.getNullTypeForDataType(searchDataType);
									}
									if (searchDataType.equalsIgnoreCase("date")
											|| searchDataType.equalsIgnoreCase("datetime")) {
										String datetime;
										if (searchDataType.equalsIgnoreCase("date")) {
											datetime = searchValue + "_00:00:00";
										} else {
											datetime = searchValue;
										}
										Long epoch = FileUtils.getDateTimeInEpoch(datetime);
										searchValue = epoch.toString();
									}
									int count = FileUtils.updateRecordsInUserTable(userDataFolderPath + tableFileName,
											searchValue, searchDataType, searchOrdinalPosition, updateValue,
											updateDataType, updateOrdinalPosition, operator, pageSize);
									if (count > 0) {
										System.out.println(count + " row(s) updated");
									} else {
										System.out.println(
												"No row(s) updated as no such " + searchColumn + " = " + searchValue);
									}

								} else {
									System.out.println("Where has a column thats not present");
								}

							} else {
								System.out.println("Set has a column which is not present");
							}
						}

					} else {
						if (isFound) {
							int count = FileUtils.updateAllRecordsInUserTable(userDataFolderPath + tableFileName,
									updateValue, updateDataType, updateOrdinalPosition, pageSize);
							if (count > 0) {
								System.out.println(count + " row(s) updated");
							}
						} else {
							System.out.println("Set has a column which is not present");
						}

					}
				} else {
					System.out.println(errorMessage);
				}
				tableFile.close();
			} else {
				System.out.println("no such table " + tableName);
			}

		} catch (Exception e) {

		}

	}

	public static void insertRecord(String fileName, ArrayList<String> data, ArrayList<String> dataType) {
		RandomAccessFile tableFile;

		try {
			int payloadLength = FileUtils.getPayloadLength(data, dataType);
			int dataLength = FileUtils.getDataLength(payloadLength);
			tableFile = new RandomAccessFile(fileName, "rw");
			tableFile.seek(0);
			int nodePage = tableFile.readInt();
			int key = tableFile.readInt() + 1;
			byte[] node = new byte[(int) (pageSize)];
			tableFile.seek(nodePage);
			tableFile.read(node, 0, (int) pageSize);
			Stack parentNodes = new Stack();
			int noOfCells = node[BtreeConstants.noOfCellsOffset];
			while ((int) node[0] != 13) {
				parentNodes.push(nodePage);
				noOfCells = node[BtreeConstants.noOfCellsOffset];
				int firstPointer = (int) BufferManagement.readShort(node, BtreeConstants.pointerOffset);
				int leftMostNodeKey = BufferManagement.readInt(node, firstPointer + 4);
				int rightPointerLoc = noOfCells * BtreeConstants.pointer - BtreeConstants.pointer;
				int rightMostPointer = (int) BufferManagement.readShort(node,
						BtreeConstants.pointerOffset + rightPointerLoc);
				int rightMostNodeKey = BufferManagement.readInt(node, rightMostPointer + 4);
				if (key <= leftMostNodeKey) {
					nodePage = BufferManagement.readInt(node, firstPointer);
				} else if (key > rightMostNodeKey) {
					nodePage = BufferManagement.readInt(node, BtreeConstants.rightOffset);

				} else {
					int nextPointerLoc = BtreeConstants.pointerOffset + 2;
					for (int i = 1; i < noOfCells; i++) {
						int nextPointer = BufferManagement.readShort(node, nextPointerLoc);
						int nodeKey = BufferManagement.readInt(node, nextPointer + 4);
						if (key <= nodeKey) {
							nodePage = BufferManagement.readInt(node, nextPointer);
							break;
						}
						nextPointerLoc = nextPointerLoc + 2;

					}
				}
				tableFile.seek(nodePage);
				tableFile.read(node, 0, (int) pageSize);
			}
			if (!FileUtils.isPageFull(node, dataLength, pageSize)) {
				node = FileUtils.insertDataInDataPage(node, key, data, dataType, pageSize);
				tableFile.seek(nodePage);
				tableFile.write(node, 0, (int) pageSize);
			} else {
				// split into two pages
				byte[] newLeafNode = FileUtils.createNewLeafPage(key, data, dataType, pageSize);
				long newLeafPageNumber = tableFile.length();
				node = FileUtils.writeRightSibling(node, newLeafPageNumber);
				// write existing page
				tableFile.seek(nodePage);
				tableFile.write(node, 0, (int) pageSize);
				// write new page
				tableFile.setLength(tableFile.length() + pageSize);
				tableFile.seek(newLeafPageNumber);
				tableFile.write(newLeafNode, 0, (int) pageSize);
				boolean finished = false;
				while (!finished) {
					if (parentNodes.size() == 0) {
						byte[] rootNode = FileUtils.createNewInternalNode(nodePage, key, newLeafPageNumber, pageSize);

						long rootPageNumber = tableFile.length();
						tableFile.setLength(tableFile.length() + pageSize);
						tableFile.seek(rootPageNumber);
						tableFile.write(rootNode, 0, (int) pageSize);
						// write root pageNumber in dummy page
						tableFile.seek(0);
						tableFile.writeInt((int) rootPageNumber);
						finished = true;
					} else {
						nodePage = (int) parentNodes.pop();
						tableFile.seek(nodePage);
						tableFile.read(node, 0, (int) pageSize);
						if (!FileUtils.isPageFull(node, BtreeConstants.interiorCellLength, pageSize)) {
							node = FileUtils.insertDataInInternalPage(node, key, pageSize, newLeafPageNumber, 0);
							tableFile.seek(nodePage);
							tableFile.write(node, 0, (int) pageSize);
							finished = true;
						} else {
							byte[] newInternalNode = FileUtils.createNewInternalNode((int) newLeafPageNumber, key,
									newLeafPageNumber, pageSize);
							newLeafPageNumber = tableFile.length();
							tableFile.seek(newLeafPageNumber);
							tableFile.write(newInternalNode, 0, (int) pageSize);
						}
					}
				}
			}
			tableFile.seek(BtreeConstants.keyOffsetInDummyPage);
			tableFile.writeInt(key);
			tableFile.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void parseCreateTable(String createTableString) {

		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split("\\(")));
		ArrayList<String> metaTokens = new ArrayList<String>(Arrays.asList(createTableTokens.get(0).split(" ")));
		String tableName = metaTokens.get(2);
		ArrayList<String> columnTokens = new ArrayList<String>(Arrays.asList(createTableTokens.get(1).split(",")));
		boolean isTablePresent = FileUtils.isTablePresent(metadataFolderPath + tablesTableFile, tableName, pageSize);
		String tableFileName = tableName + ".tbl";
		if (!isTablePresent) {
			ArrayList<String> dataArrayList = new ArrayList<String>();
			ArrayList<String> dataType = new ArrayList<String>();

			// insert into tables table
			dataArrayList.add(tableName);
			dataArrayList.add("1");// isActive
			dataType.add("text");
			dataType.add("tinyint");
			insertRecord(metadataFolderPath + tablesTableFile, dataArrayList, dataType);

			// insert into columns table
			dataType.clear();
			dataArrayList.clear();
			dataType.add("text");
			dataType.add("text");
			dataType.add("text");
			dataType.add("tinyint");
			dataType.add("text");
			dataType.add("tinyint");// isActive

			Integer ordinalPosition = 0;
			for (int i = 0; i < columnTokens.size(); i++) {
				dataArrayList.clear();
				ordinalPosition = ordinalPosition + 1;
				dataArrayList.add(tableName);
				ArrayList<String> colTokens = new ArrayList<String>(Arrays.asList(columnTokens.get(i).trim().split(" ")));
				if(i==columnTokens.size()-1) {
					colTokens.remove(colTokens.size() - 1);
				}
				dataArrayList.add(colTokens.get(0));
				dataArrayList.add(colTokens.get(1));
				dataArrayList.add(ordinalPosition.toString());
				if(colTokens.size()==3) {
					if (colTokens.get(2).equalsIgnoreCase("notnull")) {
						dataArrayList.add("NO");
					} else {
						dataArrayList.add("YES");
					}
				}
				else if(colTokens.size()==4) {
					if ((colTokens.get(2)+" "+colTokens.get(3)).equalsIgnoreCase("not null")) {
						dataArrayList.add("NO");
					} else {
						dataArrayList.add("YES");
					}
				}
				else {
					dataArrayList.add("YES");
				}							
				dataArrayList.add("1");// isActive
				insertRecord(metadataFolderPath + columnsTableFile, dataArrayList, dataType);
			}

			// create new file for table
			try {
				RandomAccessFile tableFile = new RandomAccessFile(userDataFolderPath + tableFileName, "rw");
				tableFile.setLength(pageSize * 2);
				tableFile.seek(0);
				tableFile.writeInt((int) pageSize);
				tableFile.seek(BtreeConstants.NoOfColumnsOffsetInDummyPage);
				tableFile.writeInt(ordinalPosition);
				tableFile.seek(pageSize * 1);
				tableFile.write(0x0D);
				tableFile.write(0x00);
				// no right sibling
				tableFile.seek(pageSize + BtreeConstants.rightOffset);
				tableFile.write(0xFF);
				tableFile.write(0xFF);
				tableFile.write(0xFF);
				tableFile.write(0xFF);
				tableFile.close();
				System.out.println("Table created");
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println("Table already present. Try a unique table name");
		}

	}
}