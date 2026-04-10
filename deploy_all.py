import os
import importlib.util

def register_all_spiders():
    deployments = []
    spider_dir = "./spiders"
    
    for file in os.listdir(spider_dir):
        if file.endswith(".py") and not file.startswith("__"):
            # 动态加载爬虫文件
            module_name = file[:-3]
            spec = importlib.util.spec_from_file_location(module_name, f"{spider_dir}/{file}")
            module = importlib.util.module_from_spec(spec)
            
            # 关键修复：导入 sys 并附加到 sys.modules 使得 @dataclass 能正常工作
            import sys
            sys.modules[module_name] = module
            
            spec.loader.exec_module(module)
            
            # 寻找模块中的 flow 对象
            for attr in dir(module):
                obj = getattr(module, attr)
                if hasattr(obj, "to_deployment"):
                    kwargs = {"name": f"prod-{file[:-3]}"}
                    interval = getattr(obj, "interval", None)
                    if interval is not None:
                        import datetime
                        if isinstance(interval, (int, float)):
                            kwargs["interval"] = datetime.timedelta(seconds=interval)
                        else:
                            kwargs["interval"] = interval
                    try:
                        deployments.append(obj.to_deployment(**kwargs))
                    except Exception as e:
                        print(f"Skipping {file} due to deployment error: {e}")
    
    print(f"Found {len(deployments)} deployments.")
    if not deployments:
        print("Error: No deployments found to deploy!")
        return
        
    # 逐个注册部署到 Process 类型的 work pool
    for d in deployments:
        d.work_pool_name = "docker-crawler-pool"
        d.apply()
        print(f"  Deployed: {d.name}")

if __name__ == "__main__":
    register_all_spiders()
