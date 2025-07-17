package assign;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Iterator;

public class DBApp {

	public void init() throws IOException {

		File metadata = new File("metadata.csv");
		if (!metadata.exists()) {
			metadata.createNewFile();
			FileWriter writer = new FileWriter(metadata);
			writer.close();
		}

		File filesFile = new File("files.csv");
		if (!filesFile.exists()) {
			filesFile.createNewFile();
			FileWriter filesWriter = new FileWriter(filesFile);
			filesWriter.close();
		}

		File configDir = new File("Config");
		if (!configDir.exists()) {
			configDir.mkdir();
		}

		File configFile = new File(configDir, "DBApp.properties");
		try {
			FileWriter writer = new FileWriter(configFile);
			writer.write("maxRows=200\n");
			writer.close();
		} catch (IOException e) {

		}
		
		File supportNull = new File("SupportNull.txt");
        if (!supportNull.exists()) {
            supportNull.createNewFile();
            FileWriter fileWriter = new FileWriter(supportNull);
            fileWriter.close();
        }
		
		

	}


	
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {

		File metadata = new File("metadata.csv");
		if (metadata.exists()) {
			java.util.Scanner scanner = new java.util.Scanner(metadata);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] lineArr = line.split(",");
				if (lineArr[0].equals(strTableName)) {
					scanner.close();
					throw new DBAppException("Table already exists");
				}
			}
			scanner.close();
		}

		File metadataFile = new File("metadata.csv");
		FileWriter metadataWriter = new FileWriter(metadataFile, true);

		
		boolean clusteringKeyFound = false;
		for (int i = htblColNameType.size() - 1; i >= 0; i--) {
			String columnName = (String) htblColNameType.keySet().toArray()[i];
			metadataWriter.write(strTableName + ",");
			metadataWriter.write(columnName + ",");
			String columnType = htblColNameType.get(columnName);
			metadataWriter.write(columnType + ",");
			boolean isClusteringKey = columnName.equals(strClusteringKeyColumn);
			metadataWriter.write(isClusteringKey + ",");
			metadataWriter.write(",");
			metadataWriter.write(",");
			metadataWriter.write(htblColNameMin.get(columnName) + ",");
			metadataWriter.write(htblColNameMax.get(columnName) + "\n");
			if (isClusteringKey) {
				clusteringKeyFound = true;
			}
		}

		
		if (!clusteringKeyFound) {
			metadataWriter.close();
			throw new DBAppException("Clustering key column not found");
		}

		metadataWriter.close();

		File tableFile;
		tableFile = new File(strTableName + ".csv");

		
		File filesCSV = new File("files.csv");
		FileWriter filesWriter = new FileWriter(filesCSV, true);
		File filePath = tableFile.getAbsoluteFile();
		String row = strTableName + ",Table," + strTableName + ".csv" + "," + filePath + "\n";
		filesWriter.write(row);
		filesWriter.close();
		sortFilesCSV();

		
		tableFile.createNewFile();

	}

	
	public void sortFilesCSV() throws IOException {
		
		File file = new File("files.csv");
		BufferedReader br = new BufferedReader(new FileReader(file));
		List<String> lines = br.lines().collect(Collectors.toList());
		br.close();

		
		Collections.sort(lines, new Comparator<String>() {
			public int compare(String line1, String line2) {
				String[] arr1 = line1.split(",");
				String[] arr2 = line2.split(",");
				return arr1[0].compareTo(arr2[0]);
			}
		});

		
		FileWriter writer = new FileWriter(file);
		for (String line : lines) {
			writer.write(line + "\n");
		}
		writer.close();
	}

	
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			
			File table = new File(strTableName + ".csv");
			if (!table.exists()) {
				throw new DBAppException("Table " + strTableName + " does not exist");
			}

			
			File metadata = new File("metadata.csv");
			String ck = "";
			Hashtable<String, String> columnType = new Hashtable<String, String>();
			List<String> sortedC = new ArrayList<String>();
			BufferedReader reader = new BufferedReader(new FileReader(metadata));
			String line = reader.readLine();
			while (line != null) {
				String[] arrayList = line.split(",");
				if (arrayList[0].equals(strTableName)) {
					columnType.put(arrayList[1], arrayList[2]);
					sortedC.add(arrayList[1]);
					if (Boolean.parseBoolean(arrayList[3])) {
						ck = arrayList[1];
					}
				}
				line = reader.readLine();
			}
			reader.close();

			
			if (!htblColNameValue.containsKey(ck)) {
				throw new DBAppException("Record does not contain clustering key " + ck);
			}

			
			Iterator<String> sortedCIterator = sortedC.iterator();
			while (sortedCIterator.hasNext()) {
				String column = sortedCIterator.next();
				if (!htblColNameValue.containsKey(column)) {
					htblColNameValue.put(column, "");
				} else {
					Object value = htblColNameValue.get(column);
					String type = columnType.get(column);
					if (!value.getClass().getName().equals(type)) {
						throw new DBAppException("Invalid type for column " + column + " in table " +
								strTableName);
					}
				}
			}
			
			
			File indexFile = new File(strTableName + "_sparse.csv");
			boolean indexExists = indexFile.exists();

			
			int pageNumber = 0;
			File tableFile = new File(strTableName + ".csv");
			while (!tableFile.exists()) {
				pageNumber++;
				tableFile = new File(strTableName + "_" + pageNumber + ".csv");
			}
			
			
			if (indexExists) {
				pageNumber = getPageNumberFromIndex(indexFile, htblColNameValue.get(ck).toString());
				if (pageNumber != 0) {
					tableFile = new File(strTableName + "_" + pageNumber + ".csv");
				}
			}
			
			System.out.println("Page number: " + pageNumber);
			
			
			if (!indexExists) {
				while (!tableFile.exists()) {
					pageNumber++;
					tableFile = new File(strTableName + "_" + pageNumber + ".csv");
				}

			
			while (tableFile.exists()) {
				
				int rowCount = 0;
				Set<String> clusteringKeyValues = new HashSet<String>();
				BufferedReader lastPageReader = new BufferedReader(new FileReader(tableFile));
				String lastPageLine;
				while ((lastPageLine = lastPageReader.readLine()) != null) {
					String[] parts = lastPageLine.split(",");
					String clusteringKeyValue = parts[0];
					clusteringKeyValues.add(clusteringKeyValue);
					rowCount++;
				}
				lastPageReader.close();
				if (clusteringKeyValues.contains(htblColNameValue.get(ck).toString())) {
					throw new DBAppException("Record with clustering key value " +
							htblColNameValue.get(ck).toString()
							+ " already exists in table " + strTableName);
				}
				if (rowCount < maxRows()) {
					break;
				}
				pageNumber++;
				tableFile = new File(strTableName + "_" + pageNumber + ".csv");
			}

			
			if (!tableFile.exists()) {
				tableFile.createNewFile();

				
				File files = new File("files.csv");
				BufferedReader filesReader = new BufferedReader(new FileReader(files));
				boolean found = false;
				String filesLine = filesReader.readLine();
				while (filesLine != null) {
					String[] filesArray = filesLine.split(",");
					if (filesArray[0].equals(strTableName) && filesArray[1].equals("Table")
							&& filesArray[2].equals(tableFile.getName())
							&& filesArray[3].equals(tableFile.getAbsolutePath())) {
						found = true;
						break;
					}
					filesLine = filesReader.readLine();
				}
				filesReader.close();

				if (!found) {
					BufferedWriter filesWriter = new BufferedWriter(new FileWriter(files, true));
					filesWriter
							.write(strTableName + ",Table," + tableFile.getName() + "," +
									tableFile.getAbsolutePath());
					filesWriter.newLine();
					filesWriter.close();
				}

			}
			
			}

			
			String newClusteringKeyValue = htblColNameValue.get(ck).toString();
			ClusteringKeyComparator CKC = new ClusteringKeyComparator(columnType, ck);
			BufferedReader tableReader = new BufferedReader(new FileReader(tableFile));
			List<String> newTableLines = new ArrayList<String>();
			boolean found = false;
			String tableLine;
			while ((tableLine = tableReader.readLine()) != null) {
				String[] parts = tableLine.split(",");
				String existingClusteringKeyValue = parts[0];
				if (newClusteringKeyValue.equals(existingClusteringKeyValue)) {
					tableReader.close();
					throw new DBAppException("Record with clustering key value " +
							newClusteringKeyValue
							+ " already exists in table " + strTableName);
				}
				if (CKC.compare(newClusteringKeyValue, existingClusteringKeyValue) < 0 &&
						!found) {
					found = true;
					StringBuilder newLineBuilder = new StringBuilder(newClusteringKeyValue);
					newLineBuilder.append(",");
					Iterator<String> sortedColumnsIterator2 = sortedC.iterator();
					while (sortedColumnsIterator2.hasNext()) {
						String colName = sortedColumnsIterator2.next();
						if (!colName.equals(ck)) {
							Object value = htblColNameValue.get(colName);
							if (value == null) {
								newLineBuilder.append("");
							} else {
								newLineBuilder.append(value.toString());
							}
							newLineBuilder.append(",");
						}
					}
					newLineBuilder.setLength(newLineBuilder.length() - 1);
					newTableLines.add(newLineBuilder.toString());
				}
				newTableLines.add(tableLine);
			}
			tableReader.close();
			if (!found) {
				StringBuilder newLineBuilder = new StringBuilder(newClusteringKeyValue);
				newLineBuilder.append(",");
				Iterator<String> sortedColumnsIterator2 = sortedC.iterator();
				while (sortedColumnsIterator2.hasNext()) {
					String colName = sortedColumnsIterator2.next();
					if (!colName.equals(ck)) {
						Object value = htblColNameValue.get(colName);
						if (value == null) {
							newLineBuilder.append("");
						} else {
							newLineBuilder.append(value.toString());
						}
						newLineBuilder.append(",");
					}
				}
				newLineBuilder.setLength(newLineBuilder.length() - 1);
				newTableLines.add(newLineBuilder.toString());
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile));
			for (String line2 : newTableLines) {
				writer.write(line2);
				writer.write("\n");
			}
			writer.close();
			sortTable(strTableName);

			
			int rowPosition = -1;
			try {
				BufferedReader tableReadeaho = new BufferedReader(new FileReader(tableFile));
				while (tableReadeaho.readLine() != null) {
					rowPosition++;
				}
				tableReadeaho.close();
				String sparseIndexFileName = strTableName + "_" + ck + "_sparse.csv";
	            File sparseIndexFile = new File(sparseIndexFileName);
	            if (sparseIndexFile.exists()) {
	                createIndex(strTableName, ck);
	            }
				updateIndexFiles(strTableName, htblColNameValue, tableFile.getAbsolutePath(),rowPosition);
			} catch (IOException e) {
				e.printStackTrace();	
			}
			

		} catch (IOException e) {
			throw new DBAppException("Error accessing table " + strTableName);
		}
	}
	
	private int getPageNumberFromIndex(File indexFile, String clusteringKeyValue) {
		try {
			BufferedReader indexReader = new BufferedReader(new FileReader(indexFile));
			String line;
			String prevClusteringKey = null;
			int prevPageNumber = 0;

			while ((line = indexReader.readLine()) != null) {
				String[] columns = line.split(",");
				String currentClusteringKey = columns[0];
				int currentPageNumber = Integer.parseInt(columns[1]);

				
				if (currentClusteringKey.compareTo(clusteringKeyValue) >= 0) {
					return prevPageNumber == 0 ? 0 : prevPageNumber;
				}

				prevClusteringKey = currentClusteringKey;
				prevPageNumber = currentPageNumber;
			}

			
			return prevPageNumber;

		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	private void updateIndexFiles(String tableName, Hashtable<String, Object> indexedColumns, String tableFilePath,
			int rowPosition) {
		try {
			for (Map.Entry<String, Object> indexedColumn : indexedColumns.entrySet()) {
				String columnName = indexedColumn.getKey();
				Object columnValue = indexedColumn.getValue(); 

				// Check for the existence of the index file
				File indexFile = new File(tableName + "_" + columnName + "_dense.csv");
				if (!indexFile.exists()) {
					continue; 
				}

				List<String> indexEntries = new ArrayList<>();
				BufferedReader reader = new BufferedReader(new FileReader(indexFile));
				String line = reader.readLine();
				boolean inserted = false;
				while (line != null) {
					String[] entry = line.split(",");
					Object entryColumnValue;
					if (columnValue instanceof Integer) {
						entryColumnValue = Integer.parseInt(entry[0]);
					} else { // Assuming column value can only be String or Integer
						entryColumnValue = entry[0];
					}

					if (((columnValue instanceof Integer) && ((Integer) entryColumnValue > (Integer) columnValue))
							|| ((columnValue instanceof String)
									&& (((String) entryColumnValue).compareTo((String) columnValue) > 0))
									&& !inserted) {
						
						indexEntries.add(columnValue + "," + tableFilePath + "," + rowPosition);
						inserted = true;
					}

					indexEntries.add(line);
					line = reader.readLine();
				}
				reader.close();

				if (!inserted) {
					
					indexEntries.add(columnValue + "," + tableFilePath + "," + rowPosition);
				}

				BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile));
				for (String entry : indexEntries) {
					writer.write(entry + "\n");
				}

				writer.close();
				createLevel2Index(indexFile);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
	    try {
	        
	        File tableFile = new File(strTableName + ".csv");
	        if (!tableFile.exists()) {
	            throw new DBAppException("Table " + strTableName + " does not exist.");
	        }

	        
	        File metadata = new File("metadata.csv");
	        String clusteringKey = "";
	        Hashtable<String, String> colNameType = new Hashtable<String, String>();
	        List<String> orderedColNames = new ArrayList<String>();
	        BufferedReader reader = new BufferedReader(new FileReader(metadata));
	        String line;
	        while ((line = reader.readLine()) != null) {
	            String[] parts = line.split(",");
	            if (parts[0].equals(strTableName)) {
	                colNameType.put(parts[1], parts[2]);
	                orderedColNames.add(parts[1]);
	                if (Boolean.parseBoolean(parts[3])) {
	                    clusteringKey = parts[1];
	                }
	            }
	        }
	        reader.close();

	        
	        Hashtable<String, Integer> colNameIndex = new Hashtable<String, Integer>();
	        for (String colName : htblColNameValue.keySet()) {
	            if (!colNameType.containsKey(colName)) {
	                throw new DBAppException("Column " + colName + " not found in table " + strTableName);
	            }
	            Object value = htblColNameValue.get(colName);
	            String type = colNameType.get(colName);
	            if (!value.getClass().getName().equals(type)) {
	                throw new DBAppException("Invalid type for column " + colName + " in table " + strTableName);
	            }
	            int index = orderedColNames.indexOf(colName);
	            if (index == -1) {
	                throw new DBAppException("Column " + colName + " not found in table " + strTableName);
	            }
	            colNameIndex.put(colName, index);
	        }

	        
	        File indexFile = new File(strTableName + "_" + clusteringKey + "_sparse.csv");
	        int startingPageNumber = 0;
	        int startingRowIndex = 0;

	        if (indexFile.exists()) {
	            BufferedReader indexReader = new BufferedReader(new FileReader(indexFile));
	            String indexLine;

	            System.out.println("Searching the index:");
	        
	        while ((indexLine = indexReader.readLine()) != null) {
	            String[] parts = indexLine.split(",");
	            String existingClusteringKeyValue = parts[0];
	            String filePath = parts[1];
	            int rowIndex = Integer.parseInt(parts[2]);
	            
	            System.out.println("Clustering key value: " + existingClusteringKeyValue + ", File path: " + filePath + ", Row index: " + rowIndex);

	            if (strClusteringKeyValue.compareTo(existingClusteringKeyValue) <= 0) {
	                
	                String[] filePathParts = filePath.split("\\\\"); 
	                String fileName = filePathParts[filePathParts.length - 1]; 
	                
	                
	                String[] fileNameParts = fileName.split("_");
	                String pageNumberString;
	                if (fileNameParts.length > 1) {
	                    pageNumberString = fileNameParts[1].split("\\.")[0];
	                } else {
	                    pageNumberString = "0";
	                }
	                startingPageNumber = Integer.parseInt(pageNumberString);
	                startingRowIndex = rowIndex;
	                break;
	            }
	        }
	        indexReader.close();
	        }else {
	        	System.out.println("Index file not found, starting linear search from the beginning.");
	        }
	        
	        System.out.println("Starting linear search from page: " + startingPageNumber + ", row: " + startingRowIndex);

	        
	        boolean found = false;
	        String tableLine;
	        for (int pageNumber = startingPageNumber; ; pageNumber++) {
	            File tablePage = pageNumber == 0
	                    ? new File(strTableName + ".csv")
	                    : new File(strTableName + "_" + pageNumber + ".csv");

	            if (!tablePage.exists()) {
	                break;
	            }

	            File tempFile = pageNumber == 0
	                    ? new File(strTableName + "_temp.csv")
	                    : new File(strTableName + "_temp_" + pageNumber + ".csv");
	            BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile));
	            BufferedReader pageReader = new BufferedReader(new FileReader(tablePage));

	            
	            while ((tableLine = pageReader.readLine()) != null) {
	                String[] parts = tableLine.split(",");
	                String existingClusteringKeyValue = parts[0]; 
	                if (strClusteringKeyValue.equals(existingClusteringKeyValue)) {
	                    found = true;
	                    
	                    for (String colName : orderedColNames) {
	                        if (htblColNameValue.containsKey(colName)) {
	                            int colIndex = colNameIndex.get(colName);
	                            parts[colIndex] = htblColNameValue.get(colName).toString();
	                        }
	                    }
	                    
	                    tempWriter.write(String.join(",", parts) + "\n");
	                } else {
	                    
	                    tempWriter.write(tableLine + "\n");
	                }

	            }
	            pageReader.close();
	            tempWriter.close();

	            Files.delete(tablePage.toPath());
	            Files.move(tempFile.toPath(), tablePage.toPath());
	        }

	       if (!found) {
	            throw new DBAppException("Record with clustering key value " + strClusteringKeyValue + " not found in table " + strTableName);
	        }

	    } catch (FileNotFoundException e) {
	        throw new DBAppException("Error accessing table file, metadata file, or index file: " + e.getMessage());
	    } catch (IOException e) {
	        throw new DBAppException("Error while updating table: " + e.getMessage());
	    }
	}
	

	
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			
			File tableFile = new File(strTableName + ".csv");
			if (!tableFile.exists()) {
				throw new DBAppException("Table " + strTableName + " does not exist.");
			}
	
			
			File metadata = new File("metadata.csv");
			String clusteringKey = "";
			Hashtable<String, String> colNameType = new Hashtable<String, String>();
			List<String> orderedColNames = new ArrayList<String>();
			BufferedReader reader = new BufferedReader(new FileReader(metadata));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {
					colNameType.put(parts[1], parts[2]);
					orderedColNames.add(parts[1]);
					if (Boolean.parseBoolean(parts[3])) {
						clusteringKey = parts[1];
					}
				}
			}
			reader.close();
	
			
			Hashtable<String, Integer> colNameIndex = new Hashtable<String, Integer>();
			for (String colName : htblColNameValue.keySet()) {
				if (!colNameType.containsKey(colName)) {
					throw new DBAppException("Column " + colName + " not found in table " + strTableName);
				}
				Object value = htblColNameValue.get(colName);
				String type = colNameType.get(colName);
				if (!value.getClass().getName().equals(type)) {
					throw new DBAppException("Invalid type for column " + colName + " in table " + strTableName);
				}
				int index = orderedColNames.indexOf(colName);
				if (index == -1) {
					throw new DBAppException("Column " + colName + " not found in table " + strTableName);
				}
				colNameIndex.put(colName, index);
			}
	
			
			if (!htblColNameValue.containsKey(clusteringKey)) {
				throw new DBAppException("Clustering key " + clusteringKey + " must be provided for deletion.");
			}
	
			boolean found = false;
	
			
			File indexFile = new File(strTableName + "_" + clusteringKey + "_sparse.csv");
			if (indexFile.exists()) {
				
				String strClusteringKeyValue = htblColNameValue.get(clusteringKey).toString();
				BufferedReader indexReader = new BufferedReader(new FileReader(indexFile));
				int startingPageNumber = 0;
				int startingRowIndex = 0;
				String indexLine;
	
				while ((indexLine = indexReader.readLine()) != null) {
					String[] parts = indexLine.split(",");
					String existingClusteringKeyValue = parts[0];
					String filePath = parts[1];
					int rowIndex = Integer.parseInt(parts[2]);
	
					if (strClusteringKeyValue.compareTo(existingClusteringKeyValue) <= 0) {
						
						String[] filePathParts = filePath.split("\\\\"); 
						String fileName = filePathParts[filePathParts.length - 1]; 
	
						
						String[] fileNameParts = fileName.split("_");
						String pageNumberString;
						if (fileNameParts.length > 1) {
							pageNumberString = fileNameParts[1].split("\\.")[0];
						} else {
							pageNumberString = "0";
						}
						startingPageNumber = Integer.parseInt(pageNumberString);
						startingRowIndex = rowIndex;
						break;
					}
				}
				indexReader.close();
	
				System.out.println("Starting page number: " + startingPageNumber);
				System.out.println("Starting row index: " + startingRowIndex);
	
				
				for (int pageNumber = startingPageNumber; ; pageNumber++) {
					File tablePage = pageNumber == 0
							? new File(strTableName + ".csv")
							: new File(strTableName + "_" + pageNumber + ".csv");
	
					if (!tablePage.exists()) {
						break;
					}
	
					
					File tempFile = pageNumber == 0
							? new File(strTableName + "_temp.csv")
							: new File(strTableName + "_temp_" + pageNumber + ".csv");
					BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile));
					BufferedReader pageReader = new BufferedReader(new FileReader(tablePage));
	
					
					String tableLine;
					while ((tableLine = pageReader.readLine()) != null) {
						String[] tableParts = tableLine.split(",");
						boolean match = true;
						for (String colName : htblColNameValue.keySet()) {
							int index = colNameIndex.get(colName);
							String value = htblColNameValue.get(colName).toString();
							if (!tableParts[index].equals(value)) {
								match = false;
								break;
							}
						}
	
						
						if (!match) {
							tempWriter.write(tableLine);
							tempWriter.newLine();
						} else {
							found = true;
						}
					}
	
					
					tempWriter.close();
					pageReader.close();
	
					
					if (found) {
						tablePage.delete();
						tempFile.renameTo(tablePage);
						break;
					} else {
						
						tempFile.delete();
					}
				}
			} else {
				
				for (int pageNumber = 0; ; pageNumber++) {
					File tablePage = pageNumber == 0
							? new File(strTableName + ".csv")
							: new File(strTableName + "_" + pageNumber + ".csv");
	
					if (!tablePage.exists()) {
						break;
					}
	
					
					File tempFile = pageNumber == 0
							? new File(strTableName + "_temp.csv")
							: new File(strTableName + "_temp_" + pageNumber + ".csv");
					BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile));
					BufferedReader pageReader = new BufferedReader(new FileReader(tablePage));
	
					
					String tableLine;
					while ((tableLine = pageReader.readLine()) != null) {
						String[] tableParts = tableLine.split(",");
						boolean match = true;
						for (String colName : htblColNameValue.keySet()) {
							int index = colNameIndex.get(colName);
							String value = htblColNameValue.get(colName).toString();
							if (!tableParts[index].equals(value)) {
								match = false;
								break;
							}
						}
	
						
						if (!match) {
							tempWriter.write(tableLine);
							tempWriter.newLine();
						} else {
							found = true;
						}
					}
	
					
					tempWriter.close();
					pageReader.close();
	
					
					if (found) {
						tablePage.delete();
						tempFile.renameTo(tablePage);
						break;
					} else {
						
						tempFile.delete();
					}
				}
			}
	
			
			 if (!found) {
			 	throw new DBAppException("No records found matching the specified values.");
			 }
	
		} catch (IOException e) {
			throw new DBAppException("An error occurred while accessing the table or index files.");
		}
	}
	
	
	public void deleteEmptyTablePage(String tableName, int pageNumber) throws IOException {
	    String fileName = pageNumber == 0 ? tableName + ".csv" : tableName + "_" + pageNumber + ".csv";
	    File tablePage = new File(fileName);

	    if (tablePage.exists()) {
	        BufferedReader pageReader = new BufferedReader(new FileReader(tablePage));
	        String firstLine = pageReader.readLine();
	        pageReader.close();

	        if (firstLine == null && pageNumber > 0) {
	            
	            removeTablePageFromFilesCsv(tableName, pageNumber);
	            
	            Files.delete(tablePage.toPath());
	        }
	    }
	}
	
	public void removeTablePageFromFilesCsv(String tableName, int pageNumber) throws IOException {
	    String targetFileName = pageNumber == 0 ? tableName + ".csv" : tableName + "_" + pageNumber + ".csv";
	    File filesCsv = new File("files.csv");

	    
	    File tempFilesCsv = new File("files_temp.csv");
	    BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFilesCsv));
	    BufferedReader filesReader = new BufferedReader(new FileReader(filesCsv));

	    String line;
	    while ((line = filesReader.readLine()) != null) {
	        String[] parts = line.split(",");
	        
	        if (!parts[2].equals(targetFileName)) {
	            tempWriter.write(line + "\n");
	        }
	    }
	    filesReader.close();
	    tempWriter.close();

	    
	    Files.delete(filesCsv.toPath());
	    Files.move(tempFilesCsv.toPath(), filesCsv.toPath());
	}
	

	public void sortTable(String strTableName) throws DBAppException {
		try {
			
			File metadata = new File("metadata.csv");
			String ck = "";
			Hashtable<String, String> columnType = new Hashtable<String, String>();
			List<String> sortedC = new ArrayList<String>();
			BufferedReader reader = new BufferedReader(new FileReader(metadata));
			String line = reader.readLine();
			while (line != null) {
				String[] arrayList = line.split(",");
				if (arrayList[0].equals(strTableName)) {
					columnType.put(arrayList[1], arrayList[2]);
					sortedC.add(arrayList[1]);
					if (Boolean.parseBoolean(arrayList[3])) {
						ck = arrayList[1];
					}
				}
				line = reader.readLine();
			}
			reader.close();

			
			List<String> allTableLines = new ArrayList<String>();
			int pageNumber = 0;
			File tableFile = new File(strTableName + ".csv");
			while (tableFile.exists()) {
				BufferedReader tableReader = new BufferedReader(new FileReader(tableFile));
				String tableLine;
				while ((tableLine = tableReader.readLine()) != null) {
					allTableLines.add(tableLine);
				}
				tableReader.close();

				pageNumber++;
				tableFile = new File(strTableName + "_" + pageNumber + ".csv");
			}

			
			Collections.sort(allTableLines, new Comparator<String>() {
				@Override
				public int compare(String row1, String row2) {
					
					String[] row1Cols = row1.split(",");
					String[] row2Cols = row2.split(",");

					int ck1, ck2;
					try {
						ck1 = Integer.parseInt(row1Cols[0]);
						ck2 = Integer.parseInt(row2Cols[0]);
					} catch (NumberFormatException e) {
						
						System.err.println();
						return 0;
					}

					
					return Integer.compare(ck1, ck2);
				}
			});

			
			pageNumber = 0;
			tableFile = new File(strTableName + ".csv");
			int rowIndex = 0;
			while (tableFile.exists()) {
				BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile));
				int maxRowsPerPage = maxRows();
				for (int i = 0; i < maxRowsPerPage && rowIndex < allTableLines.size(); i++, rowIndex++) {
					writer.write(allTableLines.get(rowIndex));
					writer.write("\n");
				}
				writer.close();

				pageNumber++;
				tableFile = new File(strTableName + "_" + pageNumber + ".csv");
			}
		} catch (IOException e) {
			throw new DBAppException("Error accessing table " + strTableName);
		}
	}

	class ClusteringKeyComparator implements Comparator<String> {

		private final Hashtable<String, String> colNameType;
		private final String clusteringKey;

		public ClusteringKeyComparator(Hashtable<String, String> colNameType, String clusteringKey) {
			this.colNameType = colNameType;
			this.clusteringKey = clusteringKey;
		}

		@Override
		public int compare(String value1, String value2) {
			String clusteringKeyType = colNameType.get(clusteringKey);

			if (clusteringKeyType.equals("java.lang.Integer")) {
				return Integer.compare(Integer.parseInt(value1), Integer.parseInt(value2));
			} else if (clusteringKeyType.equals("java.lang.Double")) {
				return Double.compare(Double.parseDouble(value1), Double.parseDouble(value2));
			} else if (clusteringKeyType.equals("java.util.Date")) {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				try {
					Date date1 = dateFormat.parse(value1);
					Date date2 = dateFormat.parse(value2);
					return date1.compareTo(date2);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}

			return value1.compareTo(value2);

		}

	}

	public static int maxRows() throws IOException {
		Properties props = new Properties();
		Reader reader = new FileReader("Config/DBApp.properties");
		props.load(reader);
		reader.close();
		return Integer.parseInt(props.getProperty("maxRows"));
	}

	
	public void createIndex(String strTableName, String strColName) throws DBAppException {
		try {

			File tableFile = new File(strTableName + ".csv");
			int pageNumber = 0;
			while (!tableFile.exists()) {
				pageNumber++;
				tableFile = new File(strTableName + "_" + pageNumber + ".csv");
			}
			File metadata = new File("metadata.csv");
			List<String> sortedC = new ArrayList<String>();
			BufferedReader reader = new BufferedReader(new FileReader(metadata));
			String ck = "";
			Hashtable<String, String> columnType = new Hashtable<String, String>();
			String line = reader.readLine();
			while (line != null) {
				String[] arrayList = line.split(",");
				if (arrayList[0].equals(strTableName)) {
					columnType.put(arrayList[1], arrayList[2]);
					sortedC.add(arrayList[1]);
					if (Boolean.parseBoolean(arrayList[3])) {
						ck = arrayList[1];
					}
				}
				line = reader.readLine();
			}
			reader.close();
			boolean isClusteringKey = strColName.equals(ck);
			String indexType = isClusteringKey ? "sparse" : "dense";
			File indexFile = new File(strTableName + "_" + strColName + "_" + indexType + ".csv");
			if (indexFile.exists()) {
				indexFile.delete();
			}
			indexFile.createNewFile();

			File files = new File("files.csv");
			boolean found = false;
			List<String> fileLines = Files.readAllLines(files.toPath());
			for (String fileLine : fileLines) {
				String[] fileValues = fileLine.split(",");
				if (fileValues[0].equals(strTableName)
						&& fileValues[1]
								.equals(indexType.substring(0, 1).toUpperCase() + indexType.substring(1) + "Index")
						&& fileValues[2].equals(indexFile.getName())) {
					found = true;
					break;
				}
			}
			if (!found) {
				BufferedWriter filesWriter = new BufferedWriter(new FileWriter(files, true));
				filesWriter.write(strTableName + "," + indexType.substring(0, 1).toUpperCase() + indexType.substring(1)
						+ "Index," + indexFile.getName() + "," + indexFile.getAbsolutePath());
				filesWriter.newLine();
				filesWriter.close();
			}

			int rowOffset = 0; 
			while (true) {
				String tableLine;
				RandomAccessFile table = new RandomAccessFile(tableFile, "r");
				String minVal = null;
				int fileRowOffset = 0; 
				while ((tableLine = table.readLine()) != null) {
					String[] parts = tableLine.split(",");
					String columnValue = parts[columnIndex(sortedC, strColName)];
					if (!columnValue.equals("")) {
						if (isClusteringKey || (minVal == null
								|| (columnType.get(strColName).equals("java.lang.Integer")
										&& Integer.parseInt(columnValue) < Integer.parseInt(minVal))
								|| (columnType.get(strColName).equals("java.lang.String")
										&& columnValue.compareTo(minVal) < 0))) {
							minVal = columnValue;
						}
						if (!isClusteringKey) {
							String filePath = tableFile.getAbsolutePath();
							String indexEntry = columnValue + "," + filePath + "," + (fileRowOffset + 1);
							BufferedWriter indexWriter = new BufferedWriter(new FileWriter(indexFile, true));
							indexWriter.write(indexEntry);
							indexWriter.newLine();
							indexWriter.close();
							sortDenseIndexFile(indexFile);
						}
					}
					rowOffset++; 
					fileRowOffset++; 
				}
				if (!isClusteringKey) {
					createLevel2Index(indexFile);
				}

				if (isClusteringKey && minVal != null) {
					String filePath = tableFile.getAbsolutePath();
					String indexEntry = minVal + "," + filePath + "," + (fileRowOffset);
					BufferedWriter indexWriter = new BufferedWriter(new FileWriter(indexFile, true));
					indexWriter.write(indexEntry);
					indexWriter.newLine();
					indexWriter.close();
					fileRowOffset++; 
					rowOffset++; 
				}
				table.close();
				pageNumber++;
				tableFile = new File(strTableName + "_" + pageNumber + ".csv");
				if (!tableFile.exists()) {
					break;
				}
				rowOffset = 0;

				

				
				int colIndex = sortedC.indexOf(strColName);

				
				 File metadataFile = new File("metadata.csv");
			        List<String> metadataLines = Files.readAllLines(metadataFile.toPath());
			        BufferedWriter metadataWriter = new BufferedWriter(new FileWriter(metadataFile));
			        for (String metadataLine : metadataLines) {
			            String[] metadataValues = metadataLine.split(",");
			            if (metadataValues[0].equals(strTableName) && metadataValues[1].equals(strColName)) {
			                metadataValues[4] = indexFile.getName();
			                metadataValues[5] = indexType.substring(0, 1).toUpperCase() + indexType.substring(1) + "Index";
			            }
			            metadataWriter.write(String.join(",", metadataValues));
			            metadataWriter.newLine();
			        }
			        metadataWriter.close();
			}
		} catch (IOException e) {
			throw new DBAppException("Error creating index: " + e.getMessage());
		}
	}
	
	
	
	public int columnIndex(List<String> columns, String colName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(colName)) {
                return i;
            }
        }
        return -1;
    }
	
	private void createLevel2Index(File denseIndexFile) throws IOException {
		int totalRows = countRows(denseIndexFile);
		int rowsToPick = (int) Math.ceil(totalRows * 1); 
		int rowsPerChunk = (int) Math.ceil((double) rowsToPick / 20); 

		String level2IndexFileName = denseIndexFile.getName().replace("_dense.csv", "") + "_sparse.csv";
		File level2IndexFile = new File(denseIndexFile.getParent(), level2IndexFileName);
		level2IndexFile.createNewFile();

		BufferedWriter writer = new BufferedWriter(new FileWriter(level2IndexFile));
		BufferedReader reader = new BufferedReader(new FileReader(denseIndexFile));
		String line;
		int rowIndex = 0;
		while ((line = reader.readLine()) != null && rowIndex < rowsToPick) {
			String[] parts = line.split(",");
			if (parts.length >= 3) {
				String columnValue = parts[0];
				String filePath = parts[1];
				int rowInDenseIndex = Integer.parseInt(parts[2]); 

				if (rowIndex % rowsPerChunk == 0) {
					writer.write(columnValue + "," + filePath + "," + rowInDenseIndex); 
																						
					writer.newLine();
				}
			}
			rowIndex++;
		}

		writer.close();
		reader.close();
	}
	
	private int countRows(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        int count = 0;
        while (reader.readLine() != null) {
            count++;
        }
        reader.close();
        return count;
    }


	private void sortDenseIndexFile(File indexFile) throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(indexFile.getAbsolutePath()));
		Collections.sort(lines, new Comparator<String>() {
			@Override
			public int compare(String line1, String line2) {
				String[] parts1 = line1.split(",", 3);
				String[] parts2 = line2.split(",", 3);
				String val1 = parts1[0];
				String val2 = parts2[0];
				int dataType1 = getDataType(val1);
				int dataType2 = getDataType(val2);
				if (dataType1 != dataType2) {
					return Integer.compare(dataType1, dataType2);
				} else {
					switch (dataType1) {
						case 1: 
							int num1 = Integer.parseInt(val1);
							int num2 = Integer.parseInt(val2);
							return Integer.compare(num1, num2);
						case 2: 
							float float1 = Float.parseFloat(val1);
							float float2 = Float.parseFloat(val2);
							return Float.compare(float1, float2);
						case 3: 
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							try {
								Date date1 = sdf.parse(val1);
								Date date2 = sdf.parse(val2);
								return date1.compareTo(date2);
							} catch (ParseException e) {
								e.printStackTrace();
							}
						default: 
							return val1.compareTo(val2);
					}
				}
			}
		});
		Files.write(Paths.get(indexFile.getAbsolutePath()), lines);
	}
	
	
	private int getDataType(String val) {
		try {
			Integer.parseInt(val);
			return 1; 
		} catch (NumberFormatException e1) {
			try {
				Float.parseFloat(val);
				return 2; 
			} catch (NumberFormatException e2) {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				try {
					dateFormat.parse(val);
					return 3; 
				} catch (ParseException e3) {
					return 4; 
				}
			}
		}
	}
	
	private void updateIndexFiles(String strTableName, Hashtable<String, Object> htblColNameValue, String pageFilePath)
			throws IOException, DBAppException {
		
		File table = new File(strTableName + ".csv");
		if (!table.exists()) {
			throw new DBAppException("Table " + strTableName + " does not exist");
		}

		
		File metadata = new File("metadata.csv");
		String ck = "";
		Hashtable<String, String> columnType = new Hashtable<String, String>();
		List<String> sortedC = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(metadata));
		String line = reader.readLine();
		while (line != null) {
			String[] arrayList = line.split(",");
			if (arrayList[0].equals(strTableName)) {
				columnType.put(arrayList[1], arrayList[2]);
				sortedC.add(arrayList[1]);
				if (Boolean.parseBoolean(arrayList[3])) {
					ck = arrayList[1];
				}
			}
			line = reader.readLine();
		}
		reader.close();

		
		if (!htblColNameValue.containsKey(ck)) {
			throw new DBAppException("Record does not contain clustering key " + ck);
		}

		
		Iterator<String> sortedCIterator = sortedC.iterator();
		while (sortedCIterator.hasNext()) {
			String column = sortedCIterator.next();
			if (!htblColNameValue.containsKey(column)) {
				htblColNameValue.put(column, "");
			} else {
				Object value = htblColNameValue.get(column);
				String type = columnType.get(column);
				if (!value.getClass().getName().equals(type)) {
					throw new DBAppException("Invalid type for column " + column + " in table " + strTableName);
				}
			}
		}

		
		Iterator<String> columnIterator = sortedC.iterator();
		while (columnIterator.hasNext()) {
			String column = columnIterator.next();
			File indexFile = new File(strTableName + "_" + column + "_dense.csv");
			if (indexFile.exists()) {
				
				Object value = htblColNameValue.get(column);
				BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile, true));
				long rowPosition = Files.lines(table.toPath()).count() - 1; 
				writer.write(value.toString() + "," + pageFilePath + "," + rowPosition); 
																							
				writer.newLine();
				writer.close();

				
				sortDenseIndexFile(indexFile);
				createLevel2Index(indexFile);
			}
		}
	}
	
	/*public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
	    try {
	        if (arrSQLTerms.length - 1 != strarrOperators.length) {
	            throw new DBAppException("Invalid number of operators");
	        }

	        List<Object> selectedRows = new ArrayList<>();

	        
	        String tableName = arrSQLTerms[0]._strTableName;
	        String metadataFileName = "metadata.csv";
	        List<String> columnNames = new ArrayList<>();

	        try (BufferedReader metadataReader = new BufferedReader(new FileReader(metadataFileName))) {
	            String metadataLine;
	            while ((metadataLine = metadataReader.readLine()) != null) {
	                String[] parts = metadataLine.split(",", -1); 
	                if (parts[0].trim().equalsIgnoreCase(tableName)) {
	                    columnNames.add(parts[1].trim());
	                }
	            }
	        } catch (IOException e) {
	            throw new DBAppException("Error reading metadata file: " + e.getMessage());
	        }

	        if (columnNames.isEmpty()) {
	            throw new DBAppException("Table not found in metadata");
	        }
	        
	        

	        
	        int pageNumber = 0;
	        File tableFile = new File(tableName + ".csv");
	        List<Double> pages = new ArrayList<>();
	        searchIndex(tableName, pages, metadataFileName);
	        
	        System.out.println("Pages to search: " + pages);
	        for(Double page: pages) {
	        	while (tableFile.exists()) {
	                System.out.println("Reading table file: " + tableFile.getAbsolutePath());
		            try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
		                String line;
		                while ((line = reader.readLine()) != null) {
		                    String[] values = line.split(",");

		                    boolean rowMatches = true;
		                    for (int i = 0; i < arrSQLTerms.length; i++) {
		                        int columnIndex = findColumnIndex(columnNames, arrSQLTerms[i]._strColumnName);
		                        if (columnIndex == -1) {
		                            throw new DBAppException("Column not found: " + arrSQLTerms[i]._strColumnName);
		                        }

		                        String value = values[columnIndex];
		                        String operator = arrSQLTerms[i]._strOperator;
		                        Object targetValue = arrSQLTerms[i]._objValue;

		                        boolean termMatches = compare(value, operator, targetValue);
		                        if (i > 0) {
		                            if (strarrOperators[i - 1].equalsIgnoreCase("AND")) {
		                                rowMatches = rowMatches && termMatches;
		                            } else if (strarrOperators[i - 1].equalsIgnoreCase("OR")) {
		                                rowMatches = rowMatches || termMatches;
		                            } else if (strarrOperators[i - 1].equalsIgnoreCase("XOR")) {
		                                rowMatches = rowMatches ^ termMatches;
		                            } else {
		                                throw new DBAppException("Invalid operator: " + strarrOperators[i - 1]);
		                            }
		                        } else {
		                            rowMatches = termMatches;
		                        }
		                    }
		                    

		                    if (rowMatches) {
		                        selectedRows.add(values);
		                        
		                        System.out.println("Row matches query: " + Arrays.toString(values));
		                    }
		                    
		                }
		            } catch (IOException e) {
		                throw new DBAppException("Error reading table file: " + e.getMessage());
		            }

		            
		            
		            tableFile = new File(tableName + "_" + page + ".csv");
		        }
	        }

	        

	        return selectedRows.iterator();
	    } catch (Exception e) {
	        throw new DBAppException("Error executing query: " + e.getMessage());
	    }
	    
	    
	}
	
	private int findColumnIndex(List<String> columnNames, String columnName) {
	    for (int i = 0; i < columnNames.size(); i++) {
	        if (columnNames.get(i).equalsIgnoreCase(columnName)) {
	            return i;
	        }
	    }
	    return -1;
	}
	
	private boolean compare(String value, String operator, Object targetValue) {
		
	    if (targetValue instanceof String) {
	        return compareStrings(value, operator, (String) targetValue);
	    } else if (targetValue instanceof Integer) {
	        try {
	            int intValue = Integer.parseInt(value);
	            return compareIntegers(intValue, operator, (Integer) targetValue);
	        } catch (NumberFormatException e) {
	            return false;
	        }
	    } else if (targetValue instanceof Double) {
	        try {
	            double doubleValue = Double.parseDouble(value);
	            return compareDoubles(doubleValue, operator, (Double) targetValue);
	        } catch (NumberFormatException e) {
	            return false;
	        }
	    } else {
	        throw new IllegalArgumentException("Unsupported data type: " + targetValue.getClass());
	    }
	}

	private boolean compareStrings(String value, String operator, String targetValue) {
	    int comparison = value.compareTo(targetValue);

	    switch (operator) {
	        case "=":
	            return comparison == 0;
	        case "!=":
	            return comparison != 0;
	        case ">":
	            return comparison > 0;
	        case "<":
	            return comparison < 0;
	        case ">=":
	            return comparison >= 0;
	        case "<=":
	            return comparison <= 0;
	        default:
	            throw new IllegalArgumentException("Unsupported operator: " + operator);
	    }
	}

	private boolean compareIntegers(int value, String operator, int targetValue) {
		
	    switch (operator) {
	        case "=":
	            return value == targetValue;
	        case "!=":
	            return value != targetValue;
	        case ">":
	            return value > targetValue;
	        case "<":
	            return value < targetValue;
	        case ">=":
	            return value >= targetValue;
	        case "<=":
	            return value <= targetValue;
	        default:
	            throw new IllegalArgumentException("Unsupported operator: " + operator);
	    }
	}

	private boolean compareDoubles(double value, String operator, double targetValue) {
	    switch (operator) {
	        case "=":
	            return Double.compare(value, targetValue) == 0;
	        case "!=":
	            return Double.compare(value, targetValue) != 0;
	        case ">":
	            return value > targetValue;
	        case "<":
	            return value < targetValue;
	        case ">=":
	            return value >= targetValue;
	        case "<=":
	            return value <= targetValue;
	        default:
	            throw new IllegalArgumentException("Unsupported operator: " + operator);
	    }
	}
	
	private List<Double> searchIndex(String tableName, List<Double> pages, String metadataFileName) throws DBAppException, IOException {
	    
	    String clusteringKey = "";
	    try (BufferedReader metadataReader = new BufferedReader(new FileReader(metadataFileName))) {
	        String metadataLine;
	        while ((metadataLine = metadataReader.readLine()) != null) {
	            String[] parts = metadataLine.split(",");
	            if (parts[0].equalsIgnoreCase(tableName) && Boolean.parseBoolean(parts[3])) {
	                clusteringKey = parts[1];
	                break;
	            }
	        }
	    } catch (IOException e) {
	        throw new DBAppException("Error reading metadata file: " + e.getMessage());
	    }

	    if (clusteringKey.isEmpty()) {
	        throw new DBAppException("Clustering key not found for table " + tableName);
	    }

	    
	    File indexFile = new File(tableName + "_" + clusteringKey + "_sparse.csv");

	    if (indexFile.exists()) {
	        try (BufferedReader indexReader = new BufferedReader(new FileReader(indexFile))) {
	            String indexLine;

	            while ((indexLine = indexReader.readLine()) != null) {
	                String[] parts = indexLine.split(",");
	                String filePath = parts[1];

	                
	                String[] filePathParts = filePath.split("\\\\"); 
	                String fileName = filePathParts[filePathParts.length - 1]; 

	               
	                String[] fileNameParts = fileName.split("_");
	                String pageNumberString;
	                if (fileNameParts.length > 1) {
	                    pageNumberString = fileNameParts[1].split("\\.")[0];
	                } else {
	                    pageNumberString = "0";
	                }
	                double pageNumber = Double.parseDouble(pageNumberString);
	                pages.add(pageNumber);
	            }
	        } catch (IOException e) {
	            throw new DBAppException("Error reading index file: " + e.getMessage());
	        }
	    } else {
	        System.out.println("Index file not found, starting linear search from the beginning.");
	        pages.add(0.0); 
	    }

	    return pages;
	}*/
	// Index Implementation
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
	    try {
	        if (arrSQLTerms.length - 1 != strarrOperators.length) {
	            throw new DBAppException("Invalid number of operators");
	        }

	        List<Object> selectedRows = new ArrayList<>();

	        // Read metadata
	        String tableName = arrSQLTerms[0]._strTableName;
	        String metadataFileName = "metadata.csv";
	        List<String> columnNames = new ArrayList<>();

	        try (BufferedReader metadataReader = new BufferedReader(new FileReader(metadataFileName))) {
	            String metadataLine;
	            while ((metadataLine = metadataReader.readLine()) != null) {
	                String[] parts = metadataLine.split(",", -1); // Split by comma
	                if (parts[0].trim().equalsIgnoreCase(tableName)) {
	                    columnNames.add(parts[1].trim());
	                }
	            }
	        } catch (IOException e) {
	            throw new DBAppException("Error reading metadata file: " + e.getMessage());
	        }

	        if (columnNames.isEmpty()) {
	            throw new DBAppException("Table not found in metadata");
	        }
	        
	        

	        // Read table data
	        int pageNumber = 0;
	        File tableFile = new File(tableName + ".csv");

	        while (tableFile.exists()) {
	            try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
	                String line;
	                while ((line = reader.readLine()) != null) {
	                    String[] values = line.split(",");

	                    boolean rowMatches = true;
	                    for (int i = 0; i < arrSQLTerms.length; i++) {
	                        int columnIndex = findColumnIndex(columnNames, arrSQLTerms[i]._strColumnName);
	                        if (columnIndex == -1) {
	                            throw new DBAppException("Column not found: " + arrSQLTerms[i]._strColumnName);
	                        }

	                        String value = values[columnIndex];
	                        String operator = arrSQLTerms[i]._strOperator;
	                        Object targetValue = arrSQLTerms[i]._objValue;

	                        boolean termMatches = compare(value, operator, targetValue);
	                        if (i > 0) {
	                            if (strarrOperators[i - 1].equalsIgnoreCase("AND")) {
	                                rowMatches = rowMatches && termMatches;
	                            } else if (strarrOperators[i - 1].equalsIgnoreCase("OR")) {
	                                rowMatches = rowMatches || termMatches;
	                            } else if (strarrOperators[i - 1].equalsIgnoreCase("XOR")) {
	                                rowMatches = rowMatches ^ termMatches;
	                            } else {
	                                throw new DBAppException("Invalid operator: " + strarrOperators[i - 1]);
	                            }
	                        } else {
	                            rowMatches = termMatches;
	                        }
	                    }
	                    

	                    if (rowMatches) {
	                        selectedRows.add(values);
	                    }
	                    //System.out.println("Column names: " + columnNames);
	                    //System.out.println("Read line: " + line);
	                }
	            } catch (IOException e) {
	                throw new DBAppException("Error reading table file: " + e.getMessage());
	            }

	            // Increment the page number and check for the next table file
	            pageNumber++;
	            tableFile = new File(tableName + "_" + pageNumber + ".csv");
	        }

	        return selectedRows.iterator();
	    } catch (Exception e) {
	        throw new DBAppException("Error executing query: " + e.getMessage());
	    }
	    
	    
	}
	
	private int findColumnIndex(List<String> columnNames, String columnName) {
	    for (int i = 0; i < columnNames.size(); i++) {
	        if (columnNames.get(i).equalsIgnoreCase(columnName)) {
	            return i;
	        }
	    }
	    return -1;
	}
	
	private boolean compare(String value, String operator, Object targetValue) {
		//System.out.println("Comparing value: " + value + " operator: " + operator + " targetValue: " + targetValue);
	    // Check the target value data type and convert the value accordingly
	    if (targetValue instanceof String) {
	        return compareStrings(value, operator, (String) targetValue);
	    } else if (targetValue instanceof Integer) {
	        try {
	            int intValue = Integer.parseInt(value);
	            return compareIntegers(intValue, operator, (Integer) targetValue);
	        } catch (NumberFormatException e) {
	            return false;
	        }
	    } else if (targetValue instanceof Double) {
	        try {
	            double doubleValue = Double.parseDouble(value);
	            return compareDoubles(doubleValue, operator, (Double) targetValue);
	        } catch (NumberFormatException e) {
	            return false;
	        }
	    } else {
	        throw new IllegalArgumentException("Unsupported data type: " + targetValue.getClass());
	    }
	}

	private boolean compareStrings(String value, String operator, String targetValue) {
	    int comparison = value.compareTo(targetValue);

	    switch (operator) {
	        case "=":
	            return comparison == 0;
	        case "!=":
	            return comparison != 0;
	        case ">":
	            return comparison > 0;
	        case "<":
	            return comparison < 0;
	        case ">=":
	            return comparison >= 0;
	        case "<=":
	            return comparison <= 0;
	        default:
	            throw new IllegalArgumentException("Unsupported operator: " + operator);
	    }
	}

	private boolean compareIntegers(int value, String operator, int targetValue) {
		//System.out.println("Comparing integers: " + value + " operator: " + operator + " targetValue: " + targetValue);
	    switch (operator) {
	        case "=":
	            return value == targetValue;
	        case "!=":
	            return value != targetValue;
	        case ">":
	            return value > targetValue;
	        case "<":
	            return value < targetValue;
	        case ">=":
	            return value >= targetValue;
	        case "<=":
	            return value <= targetValue;
	        default:
	            throw new IllegalArgumentException("Unsupported operator: " + operator);
	    }
	}

	private boolean compareDoubles(double value, String operator, double targetValue) {
	    switch (operator) {
	        case "=":
	            return Double.compare(value, targetValue) == 0;
	        case "!=":
	            return Double.compare(value, targetValue) != 0;
	        case ">":
	            return value > targetValue;
	        case "<":
	            return value < targetValue;
	        case ">=":
	            return value >= targetValue;
	        case "<=":
	            return value <= targetValue;
	        default:
	            throw new IllegalArgumentException("Unsupported operator: " + operator);
	    }
	}
	
	
	public static void main(String[] args) throws IOException, DBAppException, ParseException {

		String tableName = "hold";
		String clusteringKey = "id";
		Hashtable<String, String> colNameType = new Hashtable<String, String>();

		colNameType.put("name", "java.lang.String");
		colNameType.put("age", "java.lang.Integer");
		colNameType.put("GPA", "java.lang.Double");
		colNameType.put("id", "java.lang.Integer");
		

		Hashtable<String, String> colNameMin = new Hashtable<String, String>();
		colNameMin.put("id", "0");
		colNameMin.put("name", "A");
		colNameMin.put("age", "0");
		colNameMin.put("GPA", "0.0");

		Hashtable<String, String> colNameMax = new Hashtable<String, String>();
		colNameMax.put("id", "1000000");
		colNameMax.put("name", "ZZZZZZZZZZZ");
		colNameMax.put("age", "1000000");
		colNameMax.put("GPA", "4.0");

		DBApp dbApp = new DBApp();
		
		//dbApp.init();

		//dbApp.createTable(tableName, clusteringKey, colNameType,colNameMin,colNameMax);
		//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		//Date dateValue = dateFormat.parse("2001-10-14");
		HashSet<Integer> usedIds = new HashSet<Integer>();
        Random rand = new Random();
		String[] names = { "shrek", "donkey", "fiona", "gingy", "pinocchio", "Abdelrahman", "Hazem", "Puss" };
        double[] phones = { 1.11, 2.22, 3.33, 4.0};

        /*for (int i = 0; i < 1; i++) {
            int age = rand.nextInt(100);
            double GPA = phones[rand.nextInt(phones.length)];
            String name = names[rand.nextInt(names.length)];
            int id = rand.nextInt(2000);
            // Date birthdate = Date.valueOf("1990-01-01");

            Hashtable<String, Object> htblColNameValueran = new Hashtable<String, Object>();
            htblColNameValueran.put("name", name);
            htblColNameValueran.put("age", age);
            htblColNameValueran.put("GPA", 4.0);
            htblColNameValueran.put("id", 1);
            // htblColNameValueran.put("date", );

            //dbApp.insertIntoTable(tableName, htblColNameValueran);
            //dbApp.deleteFromTable(tableName, htblColNameValueran);
            System.out.println("Inserted SUCCESSFULLY");

        }*/
        int age = rand.nextInt(100);
        double GPA = phones[rand.nextInt(phones.length)];
        String name = names[rand.nextInt(names.length)];
        int id = rand.nextInt(2000);
        // Date birthdate = Date.valueOf("1990-01-01");
        for(int i = 500; i < 700; i++) {
        	Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
            htblColNameValue.put("name", name);
            htblColNameValue.put("age", age);
            htblColNameValue.put("GPA", 4.0);
            htblColNameValue.put("id", i);
            //dbApp.insertIntoTable(tableName, htblColNameValue);
            //dbApp.updateTable(tableName, "7", htblColNameValue);
            
        }
        //dbApp.createIndex(tableName, "id");
	
	Hashtable<String, Object> htblColNameValue2 = new Hashtable<String, Object>();
    htblColNameValue2.put("name", "dude");
    htblColNameValue2.put("age", 72);
    htblColNameValue2.put("GPA", 4.0);
    htblColNameValue2.put("id", 7);
    
    //dbApp.insertIntoTable(tableName, htblColNameValueran);
    //dbApp.deleteFromTable(tableName, htblColNameValueran);

		//HashSet<Integer> usedIds = new HashSet<Integer>();

		// for (int i = 0; i < 401; i++) {
		// int id = rand.nextInt(2000) + 1; // generate a random id between 2 and 501
		// while (usedIds.contains(id)) { // check if id is already used, and generate a
		// //new one if it is
		// id = rand.nextInt(500) + 2;
		// }
		// usedIds.add(id);

		//Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		//htblColNameValue.put("id", 3);
		//htblColNameValue.put("name", "test");
		//htblColNameValue.put("age", 3);
		//htblColNameValue.put("phone", 6969);
    
    //dbApp.createIndex(tableName, "name");
		
    Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
    htblColNameValue.put("name", "donkey");
    htblColNameValue.put("age", 74);
    htblColNameValue.put("GPA", 4.0);
    htblColNameValue.put("id", 240);
		Hashtable<String, Object> htblColNameValueUp = new Hashtable<String, Object>();
		 htblColNameValueUp.put("name", "dude");
		 //dbApp.deleteFromTable(tableName, htblColNameValue);
		//dbApp.insertIntoTable(tableName, htblColNameValue);
		//dbApp.updateTable(tableName, "250", htblColNameValueUp);
		//dbApp.deleteFromTable(tableName, htblColNameValue);

		//Random rand = new Random();
		// htblColNameValue.put("id",1);
		// dbApp.init();
		//dbApp.createTable(tableName, clusteringKey, colNameType,colNameMin,colNameMax);
		// dbApp.insertIntoTable(tableName, htblColNameValue);
		//String[] names = { "shrek", "donkey", "fiona", "gingy", "pinocchio", "Abdelrahman", "Hazem", "Puss" };
		//int[] phones = { 111, 222, 333, 444, 555, 666, 777, 888, 999, 000 };

		// for (int i = 0; i < 15; i++) {
		// int age = rand.nextInt(100);
		// int phone = phones[rand.nextInt(phones.length)];
		// String name = names[rand.nextInt(names.length)];

		// htblColNameValue.put("name", name);
		// htblColNameValue.put("age", age);
		// htblColNameValue.put("phone", phone);
		// htblColNameValue.put("id", i + 2);

		// dbApp.insertIntoTable(tableName, htblColNameValue);
		// }

		//dbApp.createIndex(tableName, "id");

		// for (int i = 0; i < 40; i++) {
		// int age = rand.nextInt(100);
		// int phone = phones[rand.nextInt(phones.length)];
		// String name = names[rand.nextInt(names.length)];
		// int id = rand.nextInt(200);

		// Hashtable<String, Object> htblColNameValueran = new Hashtable<String,
		// Object>();
		// htblColNameValueran.put("name", name);
		// htblColNameValueran.put("age", age);
		// htblColNameValueran.put("phone", phone);
		// htblColNameValueran.put("id", id);

		// dbApp.insertIntoTable(tableName, htblColNameValueran);

		// }
		SQLTerm[] arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "hold";
		arrSQLTerms[0]._strColumnName = "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "dude";
		
		
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[1]._strTableName = "hold";
		arrSQLTerms[1]._strColumnName = "GPA";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = 4.0;
		
		//arrSQLTerms[2] = new SQLTerm();
		//arrSQLTerms[2]._strTableName = "some";
		//arrSQLTerms[2]._strColumnName = "age";
		//arrSQLTerms[2]._strOperator = "=";
		//arrSQLTerms[2]._objValue = "32";

		String[] strarrOperators = new String[1]; 
		strarrOperators[0] = "AND";
		
		Iterator resultIterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		
		while (resultIterator.hasNext()) {
		    Object[] row = (Object[]) resultIterator.next();
		    System.out.println(Arrays.toString(row));
		}

		
		

	}

}
