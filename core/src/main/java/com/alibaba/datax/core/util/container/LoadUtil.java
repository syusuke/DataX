package com.alibaba.datax.core.util.container;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 插件加载器，大体上分reader、transformer（还未实现）和writer三中插件类型，
 * reader和writer在执行时又可能出现Job和Task两种运行时（加载的类不同）
 */
public class LoadUtil {
    private static final String pluginTypeNameFormat = "plugin.%s.%s";

    private LoadUtil() {
    }

    private enum ContainerType {
        Job("Job"), Task("Task");
        private String type;

        private ContainerType(String type) {
            this.type = type;
        }

        public String value() {
            return type;
        }
    }

    /**
     * 所有插件配置放置在pluginRegisterCenter中，为区别reader、transformer和writer，还能区别
     * 具体pluginName，故使用pluginType.pluginName作为key放置在该map中
     */
    private static Configuration pluginRegisterCenter;

    /**
     * jarLoader的缓冲
     */
    private static Map<String, JarLoader> jarLoaderCenter = new HashMap<String, JarLoader>();

    /**
     * 设置pluginConfigs，方便后面插件来获取
     *
     * @param pluginConfigs
     */
    public static void bind(Configuration pluginConfigs) {
        pluginRegisterCenter = pluginConfigs;
    }

    private static String generatePluginKey(PluginType pluginType,
                                            String pluginName) {
        return String.format(pluginTypeNameFormat, pluginType.toString(),
                pluginName);
    }

    private static Configuration getPluginConf(PluginType pluginType,
                                               String pluginName) {
        // 通过 pluginType.pluginName 获取插件的配置
        Configuration pluginConf = pluginRegisterCenter
                .getConfiguration(generatePluginKey(pluginType, pluginName));

        if (null == pluginConf) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.PLUGIN_INSTALL_ERROR,
                    String.format("DataX不能找到插件[%s]的配置.",
                            pluginName));
        }

        return pluginConf;
    }

    /**
     * 加载JobPlugin，reader、writer都可能要加载
     *
     * @param pluginType
     * @param pluginName
     * @return {@link Reader.Job},{@link Reader.Task},{@link Writer.Job},{@link Writer.Task} 子类(必须是内部类)的实例
     */
    public static AbstractJobPlugin loadJobPlugin(PluginType pluginType,
                                                  String pluginName) {

        // find class
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
                pluginType, pluginName, ContainerType.Job);

        try {
            // 反射创建 实例
            AbstractJobPlugin jobPlugin = (AbstractJobPlugin) clazz
                    .newInstance();
            // 把配置信息传到这里面去
            jobPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return jobPlugin;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("DataX找到plugin[%s]的Job配置.",
                            pluginName), e);
        }
    }

    /**
     * 加载taskPlugin，reader、writer都可能加载
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractTaskPlugin loadTaskPlugin(PluginType pluginType,
                                                    String pluginName) {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(
                pluginType, pluginName, ContainerType.Task);

        try {
            AbstractTaskPlugin taskPlugin = (AbstractTaskPlugin) clazz
                    .newInstance();
            taskPlugin.setPluginConf(getPluginConf(pluginType, pluginName));
            return taskPlugin;
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("DataX不能找plugin[%s]的Task配置.",
                            pluginName), e);
        }
    }

    /**
     * 根据插件类型、名字和执行时taskGroupId加载对应运行器
     *
     * @param pluginType
     * @param pluginName
     * @return
     */
    public static AbstractRunner loadPluginRunner(PluginType pluginType, String pluginName) {
        AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(pluginType,
                pluginName);

        switch (pluginType) {
            case READER:
                return new ReaderRunner(taskPlugin);
            case WRITER:
                return new WriterRunner(taskPlugin);
            default:
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        String.format("插件[%s]的类型必须是[reader]或[writer]!",
                                pluginName));
        }
    }

    /**
     * 反射出具体plugin实例
     *
     * @param pluginType
     * @param pluginName
     * @param pluginRunType
     * @return
     */
    @SuppressWarnings("unchecked")
    private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
            PluginType pluginType, String pluginName,
            ContainerType pluginRunType) {
        Configuration pluginConf = getPluginConf(pluginType, pluginName);
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        try {
            // 这里就是为什么在定义 plugin 时,Reader/Writer 时必须使用 内部类了,并且名字必须为 Task/Job
            // 如: com.alibaba.datax.plugin.reader.mysqlreader.MysqlReader 类中有 Task 和 Job 内部类
            return (Class<? extends AbstractPlugin>) jarLoader
                    .loadClass(pluginConf.getString("class") + "$"
                            + pluginRunType.value());
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    public static synchronized JarLoader getJarLoader(PluginType pluginType,
                                                      String pluginName) {
        // 获取 插件的配置 pluginType: reader/writer
        Configuration pluginConf = getPluginConf(pluginType, pluginName);

        // generatePluginKey ==> plugin.reader.pluginName or plugin.writer.pluginName
        JarLoader jarLoader = jarLoaderCenter.get(generatePluginKey(pluginType,
                pluginName));
        if (null == jarLoader) {
            String pluginPath = pluginConf.getString("path");
            if (StringUtils.isBlank(pluginPath)) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.RUNTIME_ERROR,
                        String.format(
                                "%s插件[%s]路径非法!",
                                pluginType, pluginName));
            }
            jarLoader = new JarLoader(new String[]{pluginPath});
            jarLoaderCenter.put(generatePluginKey(pluginType, pluginName),
                    jarLoader);
        }

        return jarLoader;
    }
}
