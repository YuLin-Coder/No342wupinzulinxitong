const base = {
    get() {
        return {
            url : "http://localhost:8080/springboot9349a/",
            name: "springboot9349a",
            // 退出到首页链接
            indexUrl: 'http://localhost:8080/springboot9349a/front/dist/index.html'
        };
    },
    getProjectName(){
        return {
            projectName: "基于Hadoop的物品租赁系统的设计与实现"
        } 
    }
}
export default base
