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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Noop Metastore cache which does not cache any data
 * All the calls are routed to the underlying delegate
 */
public class NoopMetastoreCache
        implements MetastoreCache
{
    private final ExtendedHiveMetastore delegate;

    @Inject
    public NoopMetastoreCache(
            @ForCachingHiveMetastore ExtendedHiveMetastore delegate)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<Database> getDatabase(
            MetastoreContext metastoreContext,
            String databaseName)
    {
        return delegate.getDatabase(metastoreContext, databaseName);
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        return delegate.getAllDatabases(metastoreContext);
    }

    @Override
    public Optional<Table> getTable(
            MetastoreContext metastoreContext,
            HiveTableHandle hiveTableHandle)
    {
        return delegate.getTable(metastoreContext, hiveTableHandle);
    }

    @Override
    public Optional<List<String>> getAllTables(
            MetastoreContext metastoreContext,
            String databaseName)
    {
        return delegate.getAllTables(metastoreContext, databaseName);
    }

    @Override
    public PartitionStatistics getTableStatistics(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName)
    {
        return delegate.getTableStatistics(metastoreContext, databaseName, tableName);
    }

    @Override
    public List<TableConstraint<String>> getTableConstraints(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName)
    {
        return delegate.getTableConstraints(metastoreContext, databaseName, tableName);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Set<String> partitionNames)
    {
        return delegate.getPartitionStatistics(metastoreContext, databaseName, tableName, partitionNames);
    }

    @Override
    public Optional<List<String>> getAllViews(
            MetastoreContext metastoreContext,
            String databaseName)
    {
        return delegate.getAllViews(metastoreContext, databaseName);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            List<String> partitionNames)
    {
        return delegate.getPartitionsByNames(metastoreContext, databaseName, tableName, partitionNames);
    }

    @Override
    public Optional<Partition> getPartition(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            List<String> partitionValues)
    {
        return delegate.getPartition(metastoreContext, databaseName, tableName, partitionValues);
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    @Override
    public Optional<List<String>> getPartitionNames(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName)
    {
        return delegate.getPartitionNames(metastoreContext, databaseName, tableName);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        return delegate.listTablePrivileges(metastoreContext, databaseName, tableName, principal);
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return delegate.listRoles(metastoreContext);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        return delegate.listRoleGrants(metastoreContext, principal);
    }

    @Override
    public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return delegate.lock(metastoreContext, databaseName, tableName);
    }

    @Override
    public void invalidateAll()
    {
        //NOOP
    }

    @Override
    public void invalidateTableStatisticsCache(String databaseName, String tableName)
    {
        //NOOP
    }

    @Override
    public void invalidatePartitionStatisticsCache(String databaseName, String tableName, String partitionName)
    {
        //NOOP
    }

    @Override
    public void invalidateDatabaseCache(String databaseName)
    {
        //NOOP
    }

    @Override
    public void invalidatePartitionCache(String databaseName, String tableName)
    {
        //NOOP
    }

    @Override
    public void invalidateTablePrivilegesCache(PrestoPrincipal grantee, String databaseName, String tableName)
    {
        //NOOP
    }

    @Override
    public void invalidateTableCache(String databaseName, String tableName)
    {
        //NOOP
    }

    @Override
    public void invalidateRolesCache()
    {
        //NOOP
    }

    @Override
    public void invalidateRoleGrantsCache()
    {
        //NOOP
    }
}
