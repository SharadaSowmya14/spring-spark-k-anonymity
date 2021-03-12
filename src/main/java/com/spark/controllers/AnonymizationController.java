package com.spark.controllers;

import com.spark.car_basic.DataAnonymizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("api")
@RestController
public class AnonymizationController {
  @Autowired
  DataAnonymizer dataAnonymizer;

  @RequestMapping(value="/anonymize", method = RequestMethod.POST)
  public HttpStatus anonymize() {
    dataAnonymizer.anonymize();
    return HttpStatus.OK;
  }

  @RequestMapping("/health")
  public String healthCheck() {
    return "health";
  }
}
