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

package org.apache.arrow.vector.complex;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;

public class ListViewVector extends BaseRepeatedValueVector {
  protected int validityAllocationSizeInBytes;
  protected int lastSet;
  protected CallBack callBack;
  protected Field field;
  protected ArrowBuf validityBuffer;

  public ListViewVector(String name, BufferAllocator allocator, CallBack callBack) {
    super(name, allocator, callBack);
  }

  public ListViewVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    this(new Field(name, fieldType, null), allocator, callBack);
  }

  public ListViewVector(Field field, BufferAllocator allocator, CallBack callBack) {
    super(field.getName(), allocator, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.field = field;
    this.callBack = callBack;
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
    this.lastSet = -1;
  }

  public static ListViewVector empty(String name, BufferAllocator allocator) {
    return null;
    // return new ListViewVector(name, allocator, FieldType.nullable(ArrowType.ListView.INSTANCE), null);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return null;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {

  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return null;
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {

  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return null;
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return null;
  }

  @Override
  public long getValidityBufferAddress() {
    return 0;
  }

  @Override
  public long getDataBufferAddress() {
    return 0;
  }

  @Override
  public long getOffsetBufferAddress() {
    return 0;
  }

  @Override
  public void setNull(int index) {

  }

  @Override
  public void allocateNew() throws OutOfMemoryException {

  }

  @Override
  public Field getField() {
    return null;
  }

  @Override
  public Types.MinorType getMinorType() {
    return null;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return null;
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return null;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return null;
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return null;
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return null;
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return null;
  }

  @Override
  public ArrowBuf getDataBuffer() {
    return null;
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    return null;
  }

  @Override
  public Object getObject(int index) {
    return null;
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  @Override
  public int hashCode(int index) {
    return 0;
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return 0;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return null;
    // return visitor.visit(this, value);
  }

  @Override
  public int getElementStartIndex(int index) {
    return 0;
  }

  @Override
  public int getElementEndIndex(int index) {
    return 0;
  }
}
