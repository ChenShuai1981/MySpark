package hash;

// 物理机节点模拟类，保存节点的IP、名称、端口等信息
public class Node {

    private String ip;// IP
    private String name;// 名称

    public Node(String ip, String name) {
        this.ip = ip;
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}