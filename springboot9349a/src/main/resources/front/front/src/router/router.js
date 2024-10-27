import VueRouter from 'vue-router'

//引入组件
import Index from '../pages'
import Home from '../pages/home/home'
import Login from '../pages/login/login'
import Register from '../pages/register/register'
import Center from '../pages/center/center'
import Storeup from '../pages/storeup/list'
import News from '../pages/news/news-list'
import NewsDetail from '../pages/news/news-detail'
import yonghuList from '../pages/yonghu/list'
import yonghuDetail from '../pages/yonghu/detail'
import yonghuAdd from '../pages/yonghu/add'
import gonggaoleixingList from '../pages/gonggaoleixing/list'
import gonggaoleixingDetail from '../pages/gonggaoleixing/detail'
import gonggaoleixingAdd from '../pages/gonggaoleixing/add'
import gonggaoxinxiList from '../pages/gonggaoxinxi/list'
import gonggaoxinxiDetail from '../pages/gonggaoxinxi/detail'
import gonggaoxinxiAdd from '../pages/gonggaoxinxi/add'
import wupinleibieList from '../pages/wupinleibie/list'
import wupinleibieDetail from '../pages/wupinleibie/detail'
import wupinleibieAdd from '../pages/wupinleibie/add'
import wupinxinxiList from '../pages/wupinxinxi/list'
import wupinxinxiDetail from '../pages/wupinxinxi/detail'
import wupinxinxiAdd from '../pages/wupinxinxi/add'
import wupinzulinList from '../pages/wupinzulin/list'
import wupinzulinDetail from '../pages/wupinzulin/detail'
import wupinzulinAdd from '../pages/wupinzulin/add'
import wupinguihaiList from '../pages/wupinguihai/list'
import wupinguihaiDetail from '../pages/wupinguihai/detail'
import wupinguihaiAdd from '../pages/wupinguihai/add'
import pingjiaxinxiList from '../pages/pingjiaxinxi/list'
import pingjiaxinxiDetail from '../pages/pingjiaxinxi/detail'
import pingjiaxinxiAdd from '../pages/pingjiaxinxi/add'
import wupinchuzuList from '../pages/wupinchuzu/list'
import wupinchuzuDetail from '../pages/wupinchuzu/detail'
import wupinchuzuAdd from '../pages/wupinchuzu/add'

const originalPush = VueRouter.prototype.push
VueRouter.prototype.push = function push(location) {
	return originalPush.call(this, location).catch(err => err)
}

//配置路由
export default new VueRouter({
	routes:[
		{
      path: '/',
      redirect: '/index/home'
    },
		{
			path: '/index',
			component: Index,
			children:[
				{
					path: 'home',
					component: Home
				},
				{
					path: 'center',
					component: Center,
				},
				{
					path: 'storeup',
					component: Storeup
				},
				{
					path: 'news',
					component: News
				},
				{
					path: 'newsDetail',
					component: NewsDetail
				},
				{
					path: 'yonghu',
					component: yonghuList
				},
				{
					path: 'yonghuDetail',
					component: yonghuDetail
				},
				{
					path: 'yonghuAdd',
					component: yonghuAdd
				},
				{
					path: 'gonggaoleixing',
					component: gonggaoleixingList
				},
				{
					path: 'gonggaoleixingDetail',
					component: gonggaoleixingDetail
				},
				{
					path: 'gonggaoleixingAdd',
					component: gonggaoleixingAdd
				},
				{
					path: 'gonggaoxinxi',
					component: gonggaoxinxiList
				},
				{
					path: 'gonggaoxinxiDetail',
					component: gonggaoxinxiDetail
				},
				{
					path: 'gonggaoxinxiAdd',
					component: gonggaoxinxiAdd
				},
				{
					path: 'wupinleibie',
					component: wupinleibieList
				},
				{
					path: 'wupinleibieDetail',
					component: wupinleibieDetail
				},
				{
					path: 'wupinleibieAdd',
					component: wupinleibieAdd
				},
				{
					path: 'wupinxinxi',
					component: wupinxinxiList
				},
				{
					path: 'wupinxinxiDetail',
					component: wupinxinxiDetail
				},
				{
					path: 'wupinxinxiAdd',
					component: wupinxinxiAdd
				},
				{
					path: 'wupinzulin',
					component: wupinzulinList
				},
				{
					path: 'wupinzulinDetail',
					component: wupinzulinDetail
				},
				{
					path: 'wupinzulinAdd',
					component: wupinzulinAdd
				},
				{
					path: 'wupinguihai',
					component: wupinguihaiList
				},
				{
					path: 'wupinguihaiDetail',
					component: wupinguihaiDetail
				},
				{
					path: 'wupinguihaiAdd',
					component: wupinguihaiAdd
				},
				{
					path: 'pingjiaxinxi',
					component: pingjiaxinxiList
				},
				{
					path: 'pingjiaxinxiDetail',
					component: pingjiaxinxiDetail
				},
				{
					path: 'pingjiaxinxiAdd',
					component: pingjiaxinxiAdd
				},
				{
					path: 'wupinchuzu',
					component: wupinchuzuList
				},
				{
					path: 'wupinchuzuDetail',
					component: wupinchuzuDetail
				},
				{
					path: 'wupinchuzuAdd',
					component: wupinchuzuAdd
				},
			]
		},
		{
			path: '/login',
			component: Login
		},
		{
			path: '/register',
			component: Register
		},
	]
})
