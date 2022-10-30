import os
import subprocess

def main():
    os.chdir('model')

    # Generate Clustering Model
    subprocess.run(['python', '-m', 'clustering'])
    
    print("Clustering Model Generated!")

    # Generate Forecasting Model
    # subprocess.run('python', '-m', 'forecasting')

    # print('Forecasting Model Generated!')

if __name__ == '__main__':
    main()