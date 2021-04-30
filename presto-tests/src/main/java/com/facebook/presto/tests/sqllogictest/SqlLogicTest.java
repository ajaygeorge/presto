/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.tests.sqllogictest;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.weakref.jmx.internal.guava.hash.HashFunction;
import org.weakref.jmx.internal.guava.hash.Hasher;
import org.weakref.jmx.internal.guava.hash.Hashing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.isHidden;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.walkFileTree;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public final class SqlLogicTest
{
    private static final Logger LOG = Logger.get(SqlLogicTest.class);

    public SqlLogicTestResult getResult()
    {
        return result;
    }

    private final SqlLogicTestResult result;
    private final FileVisitor<Path> fileVisitor;
    private final Path testFilesRootPath;
    private final Path resultPath;
    private final DatabaseDialect databaseDialect;
    private final int maxNumFilesToVisit;

    enum CoreSqlParsingError
    {
        PARSING_ERROR,
        UNPARSING_ERROR,
        UNPARSED_DOES_NOT_MATCH_ORIGINAL_ERROR
    }

    enum DatabaseDialect
    {
        ALL,
        ORACLE,
        MYSQL,
        MSSQL,
        SQLITE,
        POSTGRESQL
    }

    public SqlLogicTest(SqlLogicTestConfig config)
    {
        this.testFilesRootPath = requireNonNull(config.getTestFilesRootPath(), "test files root path is null");
        this.resultPath = requireNonNull(config.getResultPath(), "result path is null");
        this.databaseDialect = requireNonNull(config.getDatabaseDialect(), "database dialect is null");
        this.result = new SqlLogicTestResult(config.getDatabaseDialect());
        this.fileVisitor = new SqlLogicTestFileVisitor();
        this.maxNumFilesToVisit = config.getMaxNumFilesToVisit();
    }

    public void run(AbstractTestQueries abstractTestQueries)
            throws IOException, NoSuchAlgorithmException
    {
        List<Path> files = Files.list(testFilesRootPath)
                .filter(file -> file.toString().endsWith(".test"))
                .collect(toList());
        for (Path file : files) {
            LOG.info("Reading file : %s", file);
            List<String> lines = Files.lines(file).collect(toList());
            for (int i = 0; i < lines.size(); i++) {
                String curr = lines.get(i);
                // Dealing with comments and empty lines
                if (curr.startsWith("#") || curr.trim().isEmpty()) {
                    continue;
                }
                //Dealing with statements
                if (curr.startsWith("statement")) {
                    String[] s = curr.split(" ");
                    if ("ok".equals(s[1])) {
                        //get next line
                        i++;
                        String next = lines.get(i);
                        abstractTestQueries.computeActual(next);
                    }
                    if ("error".equals(s[1])) {
                        //get next line
                        i++;
                        String next = lines.get(i);
                        abstractTestQueries.assertQueryFails(next, ".*." + s[2] + ".*.");
                    }
                }
                if (curr.startsWith("query")) {
                    String[] s = curr.split(" ");
                    String types = s[1];
                    Class[] classArr = new Class[types.length()];
                    for (int j = 0; j < types.length(); j++) {
                        if (types.charAt(j) == 'I') {
                            classArr[j] = Integer.class;
                        }
                        if (types.charAt(j) == 'T') {
                            classArr[j] = String.class;
                        }
                        if (types.charAt(j) == 'R') {
                            classArr[j] = Double.class;
                        }
                    }
                    String sql;
                    String sqlCurr = lines.get(++i);
                    StringBuilder sqlBuilder = new StringBuilder();
                    while (!sqlCurr.equals("----")) {
                        sqlBuilder.append(sqlCurr);
                        sqlBuilder.append("\n");
                        sqlCurr = lines.get(++i);
                    }
                    sql = sqlBuilder.toString();
                    LOG.info("Running \n %s", sql);
                    MaterializedResult result = abstractTestQueries.computeActual(sql);
                    List<String> rows = new ArrayList<>();
                    i++; //Next Line
                    //i++; //Skip col names
                    while(!lines.get(i).trim().isEmpty()) {
                        rows.add(lines.get(i));
                        i++;
                    }
                    for (int j = 0; j < rows.size(); j++) {
                        String row = rows.get(j);
                        if (row.contains("hashing to")) {
                            String[] hashingTokens = row.split(" ");
                            assertEquals(result.getRowCount() * types.length(), Integer.valueOf(hashingTokens[0]).intValue());
                            Hasher hasher = Hashing.md5().newHasher();
                            for (MaterializedRow materializedRow : result.getMaterializedRows()) {
                                for (int k = 0; k < materializedRow.getFields().size(); k++) {
                                    Object object = materializedRow.getFields().get(k);
                                    hasher.putString(object.toString() + "\n", StandardCharsets.UTF_8);
                                }
                            }
                            assertEquals(hasher.hash().toString(), hashingTokens[hashingTokens.length-1], "for SQL : " + sql + "\n");
                            continue;
                        }
                        assertEquals(result.getRowCount(), rows.size(), "for SQL : " + sql + "\n");
                        Splitter splitter = Splitter.on(CharMatcher.anyOf(" ")).trimResults()
                                .omitEmptyStrings();
                        List<String> columns = splitter.splitToList(row);
                        for (int k = 0; k < classArr.length ; k++) {
                            Class type = classArr[k];
                            if (type.equals(Integer.class))  {
                                Integer integer = Integer.valueOf(columns.get(k));
                                Object field = result.getMaterializedRows().get(j).getField(k);
                                if (field instanceof Long) {
                                    field = ((Long) field).intValue();
                                }
                                assertEquals(field, integer, "for SQL : " + sql + "\n");
                            } else if (type.equals(String.class)) {
                                String st = String.valueOf(columns.get(k));
                                if (st.equals("NULL")) {
                                    assertNull(result.getMaterializedRows().get(j).getField(k), "for SQL : " + sql + "\n");
                                } else {
                                    assertEquals(result.getMaterializedRows().get(j).getField(k), st, "for SQL : " + sql + "\n");
                                }
                            } else if (type.equals(Double.class)) {
                                Double dbl = Double.valueOf(columns.get(k));
                                assertEquals(result.getMaterializedRows().get(j).getField(k), dbl, "for SQL : " + sql + "\n");
                            }
                        }
                    }
                }
            }

        }

    }

    private boolean isLineBeforeStatement(String line)
    {
        return line.startsWith("statement ok") || line.startsWith("query");
    }

    private boolean statementIsAcceptableToCurrDialect(SqlLogicTestStatement sqlLogicTestStatement)
    {
        if (databaseDialect == DatabaseDialect.ALL) {
            return sqlLogicTestStatement.getSkipIfSet().isEmpty() && !sqlLogicTestStatement.getCurrStatementsOnlyIfDialect().isPresent();
        }
        if (sqlLogicTestStatement.getCurrStatementsOnlyIfDialect().isPresent() && sqlLogicTestStatement.getCurrStatementsOnlyIfDialect().get() != databaseDialect) {
            return false;
        }
        return !sqlLogicTestStatement.getSkipIfSet().contains(databaseDialect);
    }

    private boolean isConditionalRecordLine(String line)
    {
        String[] splitLine = line.split(" ");
        if (splitLine.length <= 1) {
            return false;
        }
        String conditionalFlag = splitLine[0];
        if (!conditionalFlag.equals("skipif") && !conditionalFlag.equals("onlyif")) {
            return false;
        }
        String potentialDialect = splitLine[1];
        try {
            DatabaseDialect.valueOf(potentialDialect.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            LOG.debug("This line had a skip-if or only-if dialect that isn't supported: %s", line);
            return false;
        }
        return true;
    }

    private void processConditionalRecordLine(String line, SqlLogicTestStatement.Builder builder)
    {
        String[] splitLine = line.split(" ");
        if (splitLine.length <= 1) {
            return;
        }
        String potentialDialect = splitLine[1];
        DatabaseDialect dialect = DatabaseDialect.valueOf(potentialDialect.toUpperCase());
        if (line.startsWith("onlyif")) {
            builder.setOnlyIfDialect(dialect);
        }
        else if (line.startsWith("skipif")) {
            builder.addDialectToSkipIfSet(dialect);
        }
    }

    private static String getCleanedUpStatement(StringBuilder statement)
    {
        statement = new StringBuilder(statement.toString().trim());
        return statement.toString();
    }

    private int getStartingIndexOfNextStatement(int currIdx, List<String> statements)
    {
        while (currIdx < statements.size() && !statements.get(currIdx).equals("")) {
            ++currIdx;
        }
        return currIdx;
    }

    private int getIndexOfNextDelimiter(int currIdx, List<String> statements)
    {
        while (currIdx < statements.size() && !statements.get(currIdx).equals("----")) {
            ++currIdx;
        }
        return currIdx;
    }

    private List<List<String>> getListOfStatementsAsLines(List<String> linesFromFile)
    {
        List<List<String>> statementsAsListsOfLines = new ArrayList<>();
        int currIdx = 0;
        while (currIdx < linesFromFile.size()) {
            if (isConditionalRecordLine(linesFromFile.get(currIdx)) || isLineBeforeStatement(linesFromFile.get(currIdx))) {
                int startingIdxOfNextStatement = getStartingIndexOfNextStatement(currIdx, linesFromFile);
                int endOfCurrStatement = min(startingIdxOfNextStatement, getIndexOfNextDelimiter(currIdx, linesFromFile));
                statementsAsListsOfLines.add(linesFromFile.subList(currIdx, endOfCurrStatement));
                currIdx = startingIdxOfNextStatement;
            }
            else {
                ++currIdx;
            }
        }
        return statementsAsListsOfLines;
    }

    private SqlLogicTestStatement buildSqlLogicTestStatementFromLines(List<String> statementAsListsOfLines)
    {
        SqlLogicTestStatement.Builder builder = SqlLogicTestStatement.builder();
        StringBuilder statement = new StringBuilder();
        for (String line : statementAsListsOfLines) {
            if (isConditionalRecordLine(line)) {
                processConditionalRecordLine(line, builder);
            }
            else if (!isLineBeforeStatement(line)) {
                statement.append(line);
            }
        }
        String cleanStatement = getCleanedUpStatement(statement);
        return builder.setStatement(cleanStatement).build();
    }

    private List<SqlLogicTestStatement> parseSqlLogicTestStatementsFromFile(Path file)
            throws IOException
    {
        List<String> lines = readAllLines(file);
        List<List<String>> statementsAsListsOfLines = getListOfStatementsAsLines(lines);
        return statementsAsListsOfLines.stream()
                .map(this::buildSqlLogicTestStatementFromLines)
                .filter(this::statementIsAcceptableToCurrDialect)
                .collect(toList());
    }

    private class SqlLogicTestFileVisitor
            implements FileVisitor<Path>
    {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        {
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException
        {
            if (isHidden(file)) {
                return CONTINUE;
            }
            if (result.getNumFilesVisited() >= maxNumFilesToVisit) {
                return TERMINATE;
            }
            result.incrementNumFilesVisited();
            List<SqlLogicTestStatement> testStatements = parseSqlLogicTestStatementsFromFile(file);
            result.addTestStatements(testStatements);
            result.incrementNumStatementsFound(testStatements.size());
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exception)
        {
            LOG.warn(exception, "Failed to access file: %s", file.toString());
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exception)
        {
            return CONTINUE;
        }
    }
}
