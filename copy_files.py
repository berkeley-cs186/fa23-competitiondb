import shutil
import os

def find_directory(prefix, parent, project):
    dir = ''
    for file in os.listdir(parent):
        if file.startswith(prefix):
            if dir:
                prompt = "You have more than one project " + project + " directory. Please enter the name of your directory (ex. fa23-proj2-githubusername)"
                user_input = input(prompt)
                return find_directory(user_input, parent, project)
                
            else:
                dir = os.path.join(parent,file)

    if dir == '':
        prompt = "We could not find your project " + project +  " directory. Please enter the name of your directory (ex. fa23-proj2-githubusername)"
        user_input = input(prompt)
        return find_directory(user_input, parent, project)
    
    return dir

def copy_files():

    cwd = os.getcwd()
    parent = os.path.abspath(os.path.join(cwd, os.pardir))
    proj2_prefix = 'fa23-proj2-'
    proj3_prefix = 'fa23-proj3-'

    proj2_dir = find_directory(proj2_prefix, parent, "2")
    proj3_dir = find_directory(proj3_prefix, parent, "3")


    db_path = 'src/main/java/edu/berkeley/cs186/database/'
    proj2_files = [os.path.join(db_path,'index',p + '.java') for p in ['BPlusTree', 'InnerNode', 'LeafNode', 'BPlusNode']]
    proj3_files = [os.path.join(db_path,'query',p + '.java') for p in ['join/BNLJOperator', 'SortOperator', 'join/SortMergeOperator', 'join/GHJOperator', 'QueryPlan']]

    for file_name in proj2_files:
        source_file = os.path.join(proj2_dir, file_name)
        shutil.copy(source_file, file_name)

    for file_name in proj3_files:
        source_file = os.path.join(proj3_dir, file_name)
        shutil.copy(source_file, file_name)

copy_files()