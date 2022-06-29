package org.apache.seatunnel.admin;

import com.baomidou.mybatisplus.generator.FastAutoGenerator;
import com.baomidou.mybatisplus.generator.config.OutputFile;
import com.baomidou.mybatisplus.generator.engine.FreemarkerTemplateEngine;

import java.util.Collections;

public class FastAutoGeneratorTest {

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:12390/seatunnel_db?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&autoReconnect=true&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        String username = "root";
        String password = "hyquan2020";
        String outputDir = "D:\\tmp\\seatunnel-admin";


        FastAutoGenerator.create(url, username, password)
                .globalConfig(builder -> {
                    builder.author("quanzhian") // 设置作者
                            .enableSwagger() // 开启 swagger 模式
                            .fileOverride() // 覆盖已生成文件
                            .outputDir(outputDir); // 指定输出目录
                })
                .packageConfig(builder -> {
                    builder.parent("org.apache.seatunnel") // 设置父包名
                            .moduleName("admin") // 设置父包模块名
                            .pathInfo(Collections.singletonMap(OutputFile.xml, outputDir.concat("\\xml"))); // 设置mapperXml生成路径
                })
                .strategyConfig(builder -> {
                    builder.addTablePrefix("t_", "c_"); // 设置过滤表前缀
                })
                .templateEngine(new FreemarkerTemplateEngine()) // 使用Freemarker引擎模板，默认的是Velocity引擎模板
                .execute();

    }

}
