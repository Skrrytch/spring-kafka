package com.codenotfound.model;

public class EdigasMessage {

  private String bar;

  public EdigasMessage() {}

  public EdigasMessage(String bar) {
    this.bar = bar;
  }

  public String getBar() {
    return bar;
  }

  public void setBar(String bar) {
    this.bar = bar;
  }

  @Override
  public String toString() {
    return "Bar [bar=" + bar + "]";
  }
}
