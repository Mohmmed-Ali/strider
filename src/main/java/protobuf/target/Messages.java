package protobuf.target;
// source: Messages.proto

public final class Messages {
  private Messages() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface RDFTripleOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string s = 1;
    /**
     * <code>required string s = 1;</code>
     */
    boolean hasS();
    /**
     * <code>required string s = 1;</code>
     */
    java.lang.String getS();
    /**
     * <code>required string s = 1;</code>
     */
    com.google.protobuf.ByteString
        getSBytes();

    // required string p = 2;
    /**
     * <code>required string p = 2;</code>
     */
    boolean hasP();
    /**
     * <code>required string p = 2;</code>
     */
    java.lang.String getP();
    /**
     * <code>required string p = 2;</code>
     */
    com.google.protobuf.ByteString
        getPBytes();

    // required string o = 3;
    /**
     * <code>required string o = 3;</code>
     */
    boolean hasO();
    /**
     * <code>required string o = 3;</code>
     */
    java.lang.String getO();
    /**
     * <code>required string o = 3;</code>
     */
    com.google.protobuf.ByteString
        getOBytes();
  }
  /**
   * Protobuf type {@code RDFTriple}
   */
  public static final class RDFTriple extends
      com.google.protobuf.GeneratedMessage
      implements RDFTripleOrBuilder {
    // Use RDFTriple.newBuilder() to construct.
    private RDFTriple(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private RDFTriple(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final RDFTriple defaultInstance;
    public static RDFTriple getDefaultInstance() {
      return defaultInstance;
    }

    public RDFTriple getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private RDFTriple(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              s_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              p_ = input.readBytes();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              o_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return Messages.internal_static_RDFTriple_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return Messages.internal_static_RDFTriple_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              Messages.RDFTriple.class, Messages.RDFTriple.Builder.class);
    }

    public static com.google.protobuf.Parser<RDFTriple> PARSER =
        new com.google.protobuf.AbstractParser<RDFTriple>() {
      public RDFTriple parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new RDFTriple(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<RDFTriple> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string s = 1;
    public static final int S_FIELD_NUMBER = 1;
    private java.lang.Object s_;
    /**
     * <code>required string s = 1;</code>
     */
    public boolean hasS() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string s = 1;</code>
     */
    public java.lang.String getS() {
      java.lang.Object ref = s_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          s_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string s = 1;</code>
     */
    public com.google.protobuf.ByteString
        getSBytes() {
      java.lang.Object ref = s_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        s_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required string p = 2;
    public static final int P_FIELD_NUMBER = 2;
    private java.lang.Object p_;
    /**
     * <code>required string p = 2;</code>
     */
    public boolean hasP() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required string p = 2;</code>
     */
    public java.lang.String getP() {
      java.lang.Object ref = p_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          p_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string p = 2;</code>
     */
    public com.google.protobuf.ByteString
        getPBytes() {
      java.lang.Object ref = p_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        p_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // required string o = 3;
    public static final int O_FIELD_NUMBER = 3;
    private java.lang.Object o_;
    /**
     * <code>required string o = 3;</code>
     */
    public boolean hasO() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required string o = 3;</code>
     */
    public java.lang.String getO() {
      java.lang.Object ref = o_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          o_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string o = 3;</code>
     */
    public com.google.protobuf.ByteString
        getOBytes() {
      java.lang.Object ref = o_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        o_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      s_ = "";
      p_ = "";
      o_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasS()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasP()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasO()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getSBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getPBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, getOBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getSBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getPBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, getOBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static Messages.RDFTriple parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static Messages.RDFTriple parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static Messages.RDFTriple parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static Messages.RDFTriple parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static Messages.RDFTriple parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static Messages.RDFTriple parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static Messages.RDFTriple parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static Messages.RDFTriple parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static Messages.RDFTriple parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static Messages.RDFTriple parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(Messages.RDFTriple prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code RDFTriple}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements Messages.RDFTripleOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return Messages.internal_static_RDFTriple_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return Messages.internal_static_RDFTriple_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                Messages.RDFTriple.class, Messages.RDFTriple.Builder.class);
      }

      // Construct using Messages.RDFTriple.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        s_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        p_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        o_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return Messages.internal_static_RDFTriple_descriptor;
      }

      public Messages.RDFTriple getDefaultInstanceForType() {
        return Messages.RDFTriple.getDefaultInstance();
      }

      public Messages.RDFTriple build() {
        Messages.RDFTriple result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public Messages.RDFTriple buildPartial() {
        Messages.RDFTriple result = new Messages.RDFTriple(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.s_ = s_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.p_ = p_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.o_ = o_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof Messages.RDFTriple) {
          return mergeFrom((Messages.RDFTriple)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(Messages.RDFTriple other) {
        if (other == Messages.RDFTriple.getDefaultInstance()) return this;
        if (other.hasS()) {
          bitField0_ |= 0x00000001;
          s_ = other.s_;
          onChanged();
        }
        if (other.hasP()) {
          bitField0_ |= 0x00000002;
          p_ = other.p_;
          onChanged();
        }
        if (other.hasO()) {
          bitField0_ |= 0x00000004;
          o_ = other.o_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasS()) {
          
          return false;
        }
        if (!hasP()) {
          
          return false;
        }
        if (!hasO()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        Messages.RDFTriple parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (Messages.RDFTriple) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string s = 1;
      private java.lang.Object s_ = "";
      /**
       * <code>required string s = 1;</code>
       */
      public boolean hasS() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string s = 1;</code>
       */
      public java.lang.String getS() {
        java.lang.Object ref = s_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          s_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string s = 1;</code>
       */
      public com.google.protobuf.ByteString
          getSBytes() {
        java.lang.Object ref = s_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          s_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string s = 1;</code>
       */
      public Builder setS(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        s_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string s = 1;</code>
       */
      public Builder clearS() {
        bitField0_ = (bitField0_ & ~0x00000001);
        s_ = getDefaultInstance().getS();
        onChanged();
        return this;
      }
      /**
       * <code>required string s = 1;</code>
       */
      public Builder setSBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        s_ = value;
        onChanged();
        return this;
      }

      // required string p = 2;
      private java.lang.Object p_ = "";
      /**
       * <code>required string p = 2;</code>
       */
      public boolean hasP() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required string p = 2;</code>
       */
      public java.lang.String getP() {
        java.lang.Object ref = p_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          p_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string p = 2;</code>
       */
      public com.google.protobuf.ByteString
          getPBytes() {
        java.lang.Object ref = p_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          p_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string p = 2;</code>
       */
      public Builder setP(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        p_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string p = 2;</code>
       */
      public Builder clearP() {
        bitField0_ = (bitField0_ & ~0x00000002);
        p_ = getDefaultInstance().getP();
        onChanged();
        return this;
      }
      /**
       * <code>required string p = 2;</code>
       */
      public Builder setPBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        p_ = value;
        onChanged();
        return this;
      }

      // required string o = 3;
      private java.lang.Object o_ = "";
      /**
       * <code>required string o = 3;</code>
       */
      public boolean hasO() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required string o = 3;</code>
       */
      public java.lang.String getO() {
        java.lang.Object ref = o_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          o_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string o = 3;</code>
       */
      public com.google.protobuf.ByteString
          getOBytes() {
        java.lang.Object ref = o_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          o_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string o = 3;</code>
       */
      public Builder setO(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        o_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string o = 3;</code>
       */
      public Builder clearO() {
        bitField0_ = (bitField0_ & ~0x00000004);
        o_ = getDefaultInstance().getO();
        onChanged();
        return this;
      }
      /**
       * <code>required string o = 3;</code>
       */
      public Builder setOBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        o_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:RDFTriple)
    }

    static {
      defaultInstance = new RDFTriple(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:RDFTriple)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_RDFTriple_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_RDFTriple_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\016Messages.proto\",\n\tRDFTriple\022\t\n\001s\030\001 \002(\t" +
      "\022\t\n\001p\030\002 \002(\t\022\t\n\001o\030\003 \002(\t"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_RDFTriple_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_RDFTriple_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_RDFTriple_descriptor,
              new java.lang.String[] { "S", "P", "O", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
