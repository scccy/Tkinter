import tkinter as tk


class Application(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.var = tk.IntVar()
        self.star = tk.StringVar()
        self.end = tk.StringVar()
        self.pack()
        self.place()
        self.create_widgets()

    def create_widgets(self):
        self.balance_name = tk.Label(self)
        self.draw_name = tk.Label(self)
        
        
        self.balance_name["text"] = '余额表'
        self.balance_name["font"] = ("微软雅黑", 25)
        self.draw_name["text"] = '提现记录'
        self.draw_name["font"] = ("微软雅黑", 25)
        
        self.balance_name.place(x=100, y=100)
        
        # self.balance_name.pack(padx=30, pady=10, side='left')
        # self.draw_name.pack(padx=60, pady=10, side='left')
	    

    def say_hi(self):
        print("hi there, everyone!")

root = tk.Tk()
root.title("日报")
root.geometry('600x400')
app = Application(master=root)
app.mainloop()