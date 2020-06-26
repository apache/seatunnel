package interestinglab;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigResolveOptions;

import java.io.File;
import java.net.URL;

public class CompleteTests {

  public static void main(String[] args) throws Exception {

    System.out.println("Hello World");

    System.setProperty("dt", "20190318");
    System.setProperty("city2", "shanghai");

    CompleteTests completeTests = new CompleteTests();

    Config config = ConfigFactory.parseFile(completeTests.getFileFromResources("interestinglab/variables.conf"));

    config = config.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
      .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

    ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false);
    System.out.println(config.root().render(renderOptions));
  }

  // get file from classpath, resources folder
  private File getFileFromResources(String fileName) {

    ClassLoader classLoader = getClass().getClassLoader();

    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new IllegalArgumentException("file is not found!");
    } else {
      return new File(resource.getFile());
    }

  }
}
