/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.assign7;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class VinImpressionCounts extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"VinImpressionCounts\",\"namespace\":\"com.refactorlabs.cs378.assign7\",\"fields\":[{\"name\":\"unique_user\",\"type\":\"long\",\"default\":0},{\"name\":\"clicks\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\"}],\"default\":null},{\"name\":\"show_badge_detail\",\"type\":\"long\",\"default\":0},{\"name\":\"edit_contact_form\",\"type\":\"long\",\"default\":0},{\"name\":\"submit_contact_form\",\"type\":\"long\",\"default\":0},{\"name\":\"marketplace_srps\",\"type\":\"long\",\"default\":0},{\"name\":\"marketplace_vdps\",\"type\":\"long\",\"default\":0}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public long unique_user;
  @Deprecated public java.util.Map<java.lang.CharSequence,java.lang.Long> clicks;
  @Deprecated public long show_badge_detail;
  @Deprecated public long edit_contact_form;
  @Deprecated public long submit_contact_form;
  @Deprecated public long marketplace_srps;
  @Deprecated public long marketplace_vdps;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public VinImpressionCounts() {}

  /**
   * All-args constructor.
   */
  public VinImpressionCounts(java.lang.Long unique_user, java.util.Map<java.lang.CharSequence,java.lang.Long> clicks, java.lang.Long show_badge_detail, java.lang.Long edit_contact_form, java.lang.Long submit_contact_form, java.lang.Long marketplace_srps, java.lang.Long marketplace_vdps) {
    this.unique_user = unique_user;
    this.clicks = clicks;
    this.show_badge_detail = show_badge_detail;
    this.edit_contact_form = edit_contact_form;
    this.submit_contact_form = submit_contact_form;
    this.marketplace_srps = marketplace_srps;
    this.marketplace_vdps = marketplace_vdps;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return unique_user;
    case 1: return clicks;
    case 2: return show_badge_detail;
    case 3: return edit_contact_form;
    case 4: return submit_contact_form;
    case 5: return marketplace_srps;
    case 6: return marketplace_vdps;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: unique_user = (java.lang.Long)value$; break;
    case 1: clicks = (java.util.Map<java.lang.CharSequence,java.lang.Long>)value$; break;
    case 2: show_badge_detail = (java.lang.Long)value$; break;
    case 3: edit_contact_form = (java.lang.Long)value$; break;
    case 4: submit_contact_form = (java.lang.Long)value$; break;
    case 5: marketplace_srps = (java.lang.Long)value$; break;
    case 6: marketplace_vdps = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'unique_user' field.
   */
  public java.lang.Long getUniqueUser() {
    return unique_user;
  }

  /**
   * Sets the value of the 'unique_user' field.
   * @param value the value to set.
   */
  public void setUniqueUser(java.lang.Long value) {
    this.unique_user = value;
  }

  /**
   * Gets the value of the 'clicks' field.
   */
  public java.util.Map<java.lang.CharSequence,java.lang.Long> getClicks() {
    return clicks;
  }

  /**
   * Sets the value of the 'clicks' field.
   * @param value the value to set.
   */
  public void setClicks(java.util.Map<java.lang.CharSequence,java.lang.Long> value) {
    this.clicks = value;
  }

  /**
   * Gets the value of the 'show_badge_detail' field.
   */
  public java.lang.Long getShowBadgeDetail() {
    return show_badge_detail;
  }

  /**
   * Sets the value of the 'show_badge_detail' field.
   * @param value the value to set.
   */
  public void setShowBadgeDetail(java.lang.Long value) {
    this.show_badge_detail = value;
  }

  /**
   * Gets the value of the 'edit_contact_form' field.
   */
  public java.lang.Long getEditContactForm() {
    return edit_contact_form;
  }

  /**
   * Sets the value of the 'edit_contact_form' field.
   * @param value the value to set.
   */
  public void setEditContactForm(java.lang.Long value) {
    this.edit_contact_form = value;
  }

  /**
   * Gets the value of the 'submit_contact_form' field.
   */
  public java.lang.Long getSubmitContactForm() {
    return submit_contact_form;
  }

  /**
   * Sets the value of the 'submit_contact_form' field.
   * @param value the value to set.
   */
  public void setSubmitContactForm(java.lang.Long value) {
    this.submit_contact_form = value;
  }

  /**
   * Gets the value of the 'marketplace_srps' field.
   */
  public java.lang.Long getMarketplaceSrps() {
    return marketplace_srps;
  }

  /**
   * Sets the value of the 'marketplace_srps' field.
   * @param value the value to set.
   */
  public void setMarketplaceSrps(java.lang.Long value) {
    this.marketplace_srps = value;
  }

  /**
   * Gets the value of the 'marketplace_vdps' field.
   */
  public java.lang.Long getMarketplaceVdps() {
    return marketplace_vdps;
  }

  /**
   * Sets the value of the 'marketplace_vdps' field.
   * @param value the value to set.
   */
  public void setMarketplaceVdps(java.lang.Long value) {
    this.marketplace_vdps = value;
  }

  /** Creates a new VinImpressionCounts RecordBuilder */
  public static com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder newBuilder() {
    return new com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder();
  }
  
  /** Creates a new VinImpressionCounts RecordBuilder by copying an existing Builder */
  public static com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder newBuilder(com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder other) {
    return new com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder(other);
  }
  
  /** Creates a new VinImpressionCounts RecordBuilder by copying an existing VinImpressionCounts instance */
  public static com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder newBuilder(com.refactorlabs.cs378.assign7.VinImpressionCounts other) {
    return new com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder(other);
  }
  
  /**
   * RecordBuilder for VinImpressionCounts instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<VinImpressionCounts>
    implements org.apache.avro.data.RecordBuilder<VinImpressionCounts> {

    private long unique_user;
    private java.util.Map<java.lang.CharSequence,java.lang.Long> clicks;
    private long show_badge_detail;
    private long edit_contact_form;
    private long submit_contact_form;
    private long marketplace_srps;
    private long marketplace_vdps;

    /** Creates a new Builder */
    private Builder() {
      super(com.refactorlabs.cs378.assign7.VinImpressionCounts.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.unique_user)) {
        this.unique_user = data().deepCopy(fields()[0].schema(), other.unique_user);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.clicks)) {
        this.clicks = data().deepCopy(fields()[1].schema(), other.clicks);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.show_badge_detail)) {
        this.show_badge_detail = data().deepCopy(fields()[2].schema(), other.show_badge_detail);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.edit_contact_form)) {
        this.edit_contact_form = data().deepCopy(fields()[3].schema(), other.edit_contact_form);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.submit_contact_form)) {
        this.submit_contact_form = data().deepCopy(fields()[4].schema(), other.submit_contact_form);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.marketplace_srps)) {
        this.marketplace_srps = data().deepCopy(fields()[5].schema(), other.marketplace_srps);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.marketplace_vdps)) {
        this.marketplace_vdps = data().deepCopy(fields()[6].schema(), other.marketplace_vdps);
        fieldSetFlags()[6] = true;
      }
    }
    
    /** Creates a Builder by copying an existing VinImpressionCounts instance */
    private Builder(com.refactorlabs.cs378.assign7.VinImpressionCounts other) {
            super(com.refactorlabs.cs378.assign7.VinImpressionCounts.SCHEMA$);
      if (isValidValue(fields()[0], other.unique_user)) {
        this.unique_user = data().deepCopy(fields()[0].schema(), other.unique_user);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.clicks)) {
        this.clicks = data().deepCopy(fields()[1].schema(), other.clicks);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.show_badge_detail)) {
        this.show_badge_detail = data().deepCopy(fields()[2].schema(), other.show_badge_detail);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.edit_contact_form)) {
        this.edit_contact_form = data().deepCopy(fields()[3].schema(), other.edit_contact_form);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.submit_contact_form)) {
        this.submit_contact_form = data().deepCopy(fields()[4].schema(), other.submit_contact_form);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.marketplace_srps)) {
        this.marketplace_srps = data().deepCopy(fields()[5].schema(), other.marketplace_srps);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.marketplace_vdps)) {
        this.marketplace_vdps = data().deepCopy(fields()[6].schema(), other.marketplace_vdps);
        fieldSetFlags()[6] = true;
      }
    }

    /** Gets the value of the 'unique_user' field */
    public java.lang.Long getUniqueUser() {
      return unique_user;
    }
    
    /** Sets the value of the 'unique_user' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setUniqueUser(long value) {
      validate(fields()[0], value);
      this.unique_user = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'unique_user' field has been set */
    public boolean hasUniqueUser() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'unique_user' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearUniqueUser() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'clicks' field */
    public java.util.Map<java.lang.CharSequence,java.lang.Long> getClicks() {
      return clicks;
    }
    
    /** Sets the value of the 'clicks' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setClicks(java.util.Map<java.lang.CharSequence,java.lang.Long> value) {
      validate(fields()[1], value);
      this.clicks = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'clicks' field has been set */
    public boolean hasClicks() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'clicks' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearClicks() {
      clicks = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'show_badge_detail' field */
    public java.lang.Long getShowBadgeDetail() {
      return show_badge_detail;
    }
    
    /** Sets the value of the 'show_badge_detail' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setShowBadgeDetail(long value) {
      validate(fields()[2], value);
      this.show_badge_detail = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'show_badge_detail' field has been set */
    public boolean hasShowBadgeDetail() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'show_badge_detail' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearShowBadgeDetail() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'edit_contact_form' field */
    public java.lang.Long getEditContactForm() {
      return edit_contact_form;
    }
    
    /** Sets the value of the 'edit_contact_form' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setEditContactForm(long value) {
      validate(fields()[3], value);
      this.edit_contact_form = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'edit_contact_form' field has been set */
    public boolean hasEditContactForm() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'edit_contact_form' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearEditContactForm() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'submit_contact_form' field */
    public java.lang.Long getSubmitContactForm() {
      return submit_contact_form;
    }
    
    /** Sets the value of the 'submit_contact_form' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setSubmitContactForm(long value) {
      validate(fields()[4], value);
      this.submit_contact_form = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'submit_contact_form' field has been set */
    public boolean hasSubmitContactForm() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'submit_contact_form' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearSubmitContactForm() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /** Gets the value of the 'marketplace_srps' field */
    public java.lang.Long getMarketplaceSrps() {
      return marketplace_srps;
    }
    
    /** Sets the value of the 'marketplace_srps' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setMarketplaceSrps(long value) {
      validate(fields()[5], value);
      this.marketplace_srps = value;
      fieldSetFlags()[5] = true;
      return this; 
    }
    
    /** Checks whether the 'marketplace_srps' field has been set */
    public boolean hasMarketplaceSrps() {
      return fieldSetFlags()[5];
    }
    
    /** Clears the value of the 'marketplace_srps' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearMarketplaceSrps() {
      fieldSetFlags()[5] = false;
      return this;
    }

    /** Gets the value of the 'marketplace_vdps' field */
    public java.lang.Long getMarketplaceVdps() {
      return marketplace_vdps;
    }
    
    /** Sets the value of the 'marketplace_vdps' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder setMarketplaceVdps(long value) {
      validate(fields()[6], value);
      this.marketplace_vdps = value;
      fieldSetFlags()[6] = true;
      return this; 
    }
    
    /** Checks whether the 'marketplace_vdps' field has been set */
    public boolean hasMarketplaceVdps() {
      return fieldSetFlags()[6];
    }
    
    /** Clears the value of the 'marketplace_vdps' field */
    public com.refactorlabs.cs378.assign7.VinImpressionCounts.Builder clearMarketplaceVdps() {
      fieldSetFlags()[6] = false;
      return this;
    }

    @Override
    public VinImpressionCounts build() {
      try {
        VinImpressionCounts record = new VinImpressionCounts();
        record.unique_user = fieldSetFlags()[0] ? this.unique_user : (java.lang.Long) defaultValue(fields()[0]);
        record.clicks = fieldSetFlags()[1] ? this.clicks : (java.util.Map<java.lang.CharSequence,java.lang.Long>) defaultValue(fields()[1]);
        record.show_badge_detail = fieldSetFlags()[2] ? this.show_badge_detail : (java.lang.Long) defaultValue(fields()[2]);
        record.edit_contact_form = fieldSetFlags()[3] ? this.edit_contact_form : (java.lang.Long) defaultValue(fields()[3]);
        record.submit_contact_form = fieldSetFlags()[4] ? this.submit_contact_form : (java.lang.Long) defaultValue(fields()[4]);
        record.marketplace_srps = fieldSetFlags()[5] ? this.marketplace_srps : (java.lang.Long) defaultValue(fields()[5]);
        record.marketplace_vdps = fieldSetFlags()[6] ? this.marketplace_vdps : (java.lang.Long) defaultValue(fields()[6]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
