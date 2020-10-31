package com.processor.gcpprocessor.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.charset.Charset;

@RestController
public class HelloWorldController {

    @Value("gs://apt-index-293821_cloudbuild/my-file.txt")
    private Resource gcsFile;

    @GetMapping("/")
    String hello() throws IOException {
        return StreamUtils.copyToString(
                this.gcsFile.getInputStream(),
                Charset.defaultCharset()) + "\n";
    }
}
