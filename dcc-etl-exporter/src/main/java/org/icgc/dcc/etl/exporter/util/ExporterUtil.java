package org.icgc.dcc.etl.exporter.util;

import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.icgc.dcc.downloader.core.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList.Builder;


final public class ExporterUtil {

	private static final Map<String, List<String>> DirectoryToDataTypeMapping = ImmutableMap
			.<String, List<String>>builder() //
			.put("donor", newArrayList(DataType.CLINICAL.indexName, DataType.CLINICALSAMPLE.indexName)) //
			.put("cnsm", newArrayList(DataType.CNSM.indexName)) //
			.put("exp_array", newArrayList(DataType.EXP_ARRAY.indexName)) //
			.put("exp_seq", newArrayList(DataType.EXP_SEQ.indexName)) //
			.put("jcn", newArrayList(DataType.JCN.indexName)) //
			.put("meth_array", newArrayList(DataType.METH_ARRAY.indexName)) //
			.put("meth_seq", newArrayList(DataType.METH_SEQ.indexName)) //
			.put("mirna_seq", newArrayList(DataType.MIRNA_SEQ.indexName)) //
			.put("pexp", newArrayList(DataType.PEXP.indexName)) //
			.put("sgv", newArrayList(DataType.SGV_CONTROLLED.indexName)) //
			.put("ssm", newArrayList(DataType.SSM_OPEN.indexName,DataType.SSM_CONTROLLED.indexName)) //
			.put("stsm", newArrayList(DataType.STSM.indexName)) //
			.build();

	@SneakyThrows
	public static List<String> retrieveDataTypesAvailable(String loaderPath,
			Configuration conf) {
		Builder<String> types = ImmutableList.builder();
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(
				loaderPath), false);
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			types.addAll(DirectoryToDataTypeMapping.get(file.getPath()
					.getName()));
		}
		return types.build();
	}

}
