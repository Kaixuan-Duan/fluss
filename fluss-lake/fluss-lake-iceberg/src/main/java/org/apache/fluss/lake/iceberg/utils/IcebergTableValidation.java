/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;

/**
 * Utils to verify whether the existing Iceberg table is compatible with the table to be created.
 *
 * <p>This class mirrors the pattern of {@code PaimonTableValidation} for the Iceberg lake format,
 * centralizing all compatibility checks in one place.
 */
public class IcebergTableValidation {

    /**
     * Validates that an existing Iceberg table is compatible with the Fluss table being created.
     *
     * @param existingTable the existing Iceberg table loaded from the catalog
     * @param expectedSchema the Iceberg schema expected by Fluss (converted from Fluss table
     *     descriptor)
     * @param expectedPartitionSpec the PartitionSpec expected by Fluss
     * @param expectedSortOrder the SortOrder expected by Fluss
     * @param expectedProperties the table properties expected by Fluss
     * @param context the LakeCatalog context providing creation metadata
     * @param tablePath the table path (for error messages)
     * @throws TableAlreadyExistException if any compatibility check fails
     */
    public static void validateExistingTable(
            Table existingTable,
            Schema expectedSchema,
            PartitionSpec expectedPartitionSpec,
            SortOrder expectedSortOrder,
            Map<String, String> expectedProperties,
            LakeCatalog.Context context,
            TablePath tablePath)
            throws TableAlreadyExistException {

        // 1. Schema compatibility
        if (!isIcebergSchemaCompatible(existingTable.schema(), expectedSchema)) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The table %s already exists in Iceberg, but the schema is not compatible. "
                                    + "Existing schema: %s, expected schema: %s. "
                                    + "Please first drop the table in Iceberg or use a new table name.",
                            tablePath, existingTable.schema(), expectedSchema));
        }

        // Steps 2-4 only apply when creating a new Fluss table
        if (context.isCreatingFlussTable()) {
            // 2. Partition spec compatibility
            if (!isPartitionSpecCompatible(existingTable.spec(), expectedPartitionSpec)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg, but the partition spec is not compatible. "
                                        + "Existing: %s, expected: %s. "
                                        + "Please first drop the table in Iceberg or use a new table name.",
                                tablePath, existingTable.spec(), expectedPartitionSpec));
            }

            // 3. Sort order compatibility
            if (!isSortOrderCompatible(
                    existingTable.sortOrder(), expectedSortOrder, existingTable.schema())) {
                throw new TableAlreadyExistException(
                        String.format(
                                "The table %s already exists in Iceberg, but the sort order is not compatible. "
                                        + "Existing: %s, expected: %s. "
                                        + "Please first drop the table in Iceberg or use a new table name.",
                                tablePath, existingTable.sortOrder(), expectedSortOrder));
            }

            // 4. Empty table check
            checkTableIsEmpty(existingTable, tablePath);
        }

        // 5. Table properties compatibility
        if (!arePropertiesCompatible(existingTable.properties(), expectedProperties)) {
            throw new TableAlreadyExistException(
                    String.format(
                            "The table %s already exists in Iceberg, but the table properties are not compatible. "
                                    + "Existing properties: %s, expected properties: %s. "
                                    + "Please first drop the table in Iceberg or use a new table name.",
                            tablePath, existingTable.properties(), expectedProperties));
        }
    }

    /**
     * Checks whether two Iceberg schemas are compatible, ignoring field IDs via {@link
     * TypeUtil#assignIncreasingFreshIds}. If the existing schema lacks system columns, only user
     * columns are compared.
     */
    public static boolean isIcebergSchemaCompatible(Schema existingSchema, Schema expectedSchema) {
        Schema normalizedExisting = TypeUtil.assignIncreasingFreshIds(existingSchema);
        Schema normalizedExpected = TypeUtil.assignIncreasingFreshIds(expectedSchema);

        List<Types.NestedField> existingFields = normalizedExisting.columns();
        List<Types.NestedField> expectedFields = normalizedExpected.columns();

        // If existing lacks system columns, compare only user columns; otherwise compare all
        int systemColumnCount = IcebergLakeCatalog.SYSTEM_COLUMNS.size();
        int expectedUserColumnCount = expectedFields.size() - systemColumnCount;
        int compareCount;
        if (existingFields.size() == expectedFields.size()) {
            compareCount = existingFields.size();
        } else if (existingFields.size() == expectedUserColumnCount) {
            compareCount = expectedUserColumnCount;
        } else {
            return false;
        }

        for (int i = 0; i < compareCount; i++) {
            Types.NestedField existing = existingFields.get(i);
            Types.NestedField expected = expectedFields.get(i);
            if (!existing.name().equals(expected.name())
                    || !existing.type().equals(expected.type())
                    || existing.isOptional() != expected.isOptional()) {
                return false;
            }
        }
        return normalizedExisting
                .identifierFieldNames()
                .equals(normalizedExpected.identifierFieldNames());
    }

    /**
     * Checks whether the existing Iceberg table has no snapshot history. Required when creating a
     * new Fluss table because historical data files lack Fluss system columns.
     */
    public static void checkTableIsEmpty(Table table, TablePath tablePath) {
        // Table has snapshots => not empty
        if (!table.snapshots().iterator().hasNext()) {
            return;
        }
        throw new TableAlreadyExistException(
                String.format(
                        "The Iceberg table %s already exists and is not empty. "
                                + "Cannot attach a new Fluss table to a non-empty Iceberg table. "
                                + "Please use an existing Fluss table with datalake enabled, "
                                + "or first drop the Iceberg table.",
                        tablePath));
    }

    /**
     * Checks partition spec compatibility by comparing source column names, field names, and
     * transforms, ignoring source IDs which differ due to Iceberg field ID reassignment.
     */
    public static boolean isPartitionSpecCompatible(
            PartitionSpec existing, PartitionSpec expected) {
        if (existing.fields().size() != expected.fields().size()) {
            return false;
        }
        for (int i = 0; i < existing.fields().size(); i++) {
            PartitionField existingField = existing.fields().get(i);
            PartitionField expectedField = expected.fields().get(i);
            if (!existingField.name().equals(expectedField.name())
                    || !existingField.transform().equals(expectedField.transform())) {
                return false;
            }
            // Also verify source column names match (resolved by full column name since sourceIds
            // differ)
            String existingSourceName = existing.schema().findColumnName(existingField.sourceId());
            String expectedSourceName = expected.schema().findColumnName(expectedField.sourceId());
            if (existingSourceName == null
                    || expectedSourceName == null
                    || !existingSourceName.equals(expectedSourceName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks sort order compatibility by comparing directions and source column names, resolved by
     * name since source IDs differ due to Iceberg field ID reassignment.
     */
    public static boolean isSortOrderCompatible(
            SortOrder existing, SortOrder expected, Schema existingSchema) {
        if (existing.fields().size() != expected.fields().size()) {
            return false;
        }
        for (int i = 0; i < existing.fields().size(); i++) {
            SortField existingField = existing.fields().get(i);
            SortField expectedField = expected.fields().get(i);
            if (existingField.direction() != expectedField.direction()
                    || existingField.nullOrder() != expectedField.nullOrder()
                    || !existingField.transform().equals(expectedField.transform())) {
                return false;
            }
            // Resolve source column names by full column path since sourceIds differ across schemas
            String expectedSourceName = expected.schema().findColumnName(expectedField.sourceId());
            String existingSourceName = existingSchema.findColumnName(existingField.sourceId());
            if (expectedSourceName == null
                    || existingSourceName == null
                    || !existingSourceName.equals(expectedSourceName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks table properties compatibility by only comparing keys present in both existing and
     * expected. Conflicting values for shared keys are not allowed. Missing keys in existing or
     * extra keys in existing are ignored.
     */
    public static boolean arePropertiesCompatible(
            Map<String, String> existingProperties, Map<String, String> expectedProperties) {
        for (Map.Entry<String, String> entry : expectedProperties.entrySet()) {
            String key = entry.getKey();
            if (existingProperties.containsKey(key)
                    && !existingProperties.get(key).equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
}
