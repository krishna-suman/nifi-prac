/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kris.processors.nifi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SideEffectFree
public class MyProcessor extends AbstractProcessor {

	private static Logger LOG = Logger.getLogger(MyProcessor.class);

	public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor.Builder().name("MY_PROPERTY")
			.displayName("My property").description("Example Property").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("SUCCESS FLOW")
			.build();
	public static final Relationship FAILURE = new Relationship.Builder().name("failure").description("failure FLOW")
			.build();
	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(MY_PROPERTY);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		relationships.add(FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

		final List<Byte> bytesList = new ArrayList<Byte>();

		AtomicBoolean error = new AtomicBoolean();

		FlowFile flowfile = session.get();
		LOG.info("flowfile Id: "+flowfile.getId()+",filename:"+flowfile.getAttribute("filename"));

		session.read(flowfile, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
				try {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				    int nRead;
				    byte[] data = new byte[1024];
				    while ((nRead = in.read(data, 0, data.length)) != -1) {
				        buffer.write(data, 0, nRead);
				    }				 
				    buffer.flush();
				    byte[] byteArray = buffer.toByteArray();
				    for(int i =0;i<byteArray.length;i++) {
				    	bytesList.add(byteArray[i]);
				    }
				    				    
				} catch (Exception ex) {
					error.set(true);
					ex.printStackTrace();
					LOG.error("Failed converting JSON " + ex.toString());
				}
			}
		});

		if (error.get()) {
			session.transfer(flowfile, FAILURE);
		} else {

			// To write the results back out ot flow file
			flowfile = session.write(flowfile, new OutputStreamCallback() {

				@Override
				public void process(OutputStream out) throws IOException {
					int size= bytesList.size();
					byte[] outbytes = new byte[size];
					for(int i =0;i<size;i++) {
				    	outbytes[i]= bytesList.get(i);
				    }
					out.write(outbytes);
				}
			});

			session.transfer(flowfile, SUCCESS);
		}

	}
}
