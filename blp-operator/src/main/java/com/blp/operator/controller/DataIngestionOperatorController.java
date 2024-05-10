package com.blp.operator.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.blp.operator.DataIngestionOperator;
import com.blp.operator.DataIngestionRequest;

/** */
@RestController
@RequestMapping("/ingestion")
public class DataIngestionOperatorController {

    @Autowired DataIngestionOperator operator;

    @PostMapping("/submit")
    Boolean submit(@RequestBody DataIngestionRequest request) {
        return operator.submit(request);
    }
}
